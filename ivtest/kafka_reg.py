#!/usr/bin/env python3
'''
usage:
    kafka_reg
    kafka_reg <list-paths>...

<list-paths> is a list of files in the current working directory that
            each contain a list of tests. by convention, the file has the
            suffix ".list". the files will be processed in order, so tests
            can be overridden if listed twice. if no files are given, a
            default list is used.
'''

import os
import sys
import json
import argparse
import test_lists
import run_ivl

from confluent_kafka import Consumer, KafkaException, AdminClient

class invalidtesttype(Exception):
    '''Exception to raise when the test type is not supported.'''
    def __init__(self, test, ttype, msg='invalid test type!'):
        self.test = test
        self.ttype = ttype
        self.msg = msg
        super().__init__(self.msg)

    def __str__(self):
        # pylint: disable-next=consider-using-f-string
        return "given test type '{ttype}' for test {test}".format(ttype=self.ttype,test=self.test)

class invalidjson(Exception):
    '''Exception to raise if the json is not parsed properly.'''
    def __init__(self, test, path, msg='invalid json file!'):
        self.test = test
        self.path = path
        self.msg = msg
        super().__init__(self.msg)

    def __str__(self):
        # pylint: disable-next=consider-using-f-string
        res = "unable to parse json file '{path}' for test {test}:".format(path=self.path,
                                                                           test=self.test)
        # pylint: disable-next=consider-using-f-string
        res += "\n    {msg}".format(msg=self.msg)
        return res


def process_overrides(group: str, it_dict: dict, it_opts: dict):
    '''override the gold file, type or arguments if needed.'''
    if group in it_dict:
        overrides = ['gold', 'type']
        for override in overrides:
            if override in it_dict[group]:
                if override == 'gold' and it_dict[group][override] == "":
                    it_opts[override] = None
                else:
                    it_opts[override] = it_dict[group][override]
        if 'iverilog-args' in it_dict[group]:
            it_opts['iverilog_args'].extend(it_dict[group]['iverilog-args'])


def force_gen(it_opts: dict):
    '''remove the current generation and force it to the latest.'''
    generations = ['-g2023', '-g2017', '-g2012', '-g2009', '-g2005-sv',
                   '-g2005', '-g2001-noconfig', '-g2001', '-g1995',
                   '-g2', '-g1']
    for gen in generations:
        if gen in it_opts['iverilog_args']:
            it_opts['iverilog_args'].remove(gen)

    if '-g2x' in it_opts['iverilog_args']:
        idx_to_replace = it_opts['iverilog_args'].index('-g2x')
        it_opts[idx_to_replace] = '-gicarus-misc'

    it_opts['iverilog_args'].insert(0, '-g2023')


def process_test(item: list, cfg: list) -> str:
    '''process a single test

    this takes in the list of tokens from the tests list file, and converts
    them (interprets them) to a collection of values.'''

    # this is the name of the test, and the name of the main sorce file
    it_key = item[0]
    test_path = item[1]
    with open(test_path, 'rt', encoding='ascii') as fd:
        try:
            it_dict = json.load(fd)
        except json.decoder.JSONDecodeError as Exception:
            raise invalidjson(it_key, test_path, Exception) from Exception

    # wrap all of this into an options dictionary for ease of handling.
    it_opts = {
        'key'               : it_key,
        'type'              : it_dict['type'],
        'iverilog_args'     : it_dict.get('iverilog-args', [ ]),
        'source'            : os.path.join("kafka_tests", it_dict['source']),
        'modulename'        : None,
        'gold'              : it_dict.get('gold', None),
        'diff'              : None,
        'vvp_args'          : [ ],
        'vvp_args_extended' : [ ]
    }

    if cfg['strict']:
        it_opts['iverilog_args'].append("-gstrict-expr-width")
        it_opts['vvp_args_extended'].append("-compatible")
        process_overrides('strict', it_dict, it_opts)
    else:
        it_opts['iverilog_args'].append('-D__ICARUS_UNSIZED__')

    if cfg['force-sv']:
        force_gen(it_opts)
        process_overrides('force-sv', it_dict, it_opts)

    if cfg['vlog95']:
        process_overrides('vlog95', it_dict, it_opts)

    # get the overridden test type.
    it_type = it_opts['type']

    if it_type == "NI":
        res = [0, "not implemented."]

    elif it_type == "normal":
        res = run_ivl.run_normal(it_opts, cfg)

    elif it_type == "CE":
        res = run_ivl.run_ce(it_opts, cfg)

    elif it_type == "EF":
        res = run_ivl.run_ef(it_opts, cfg)

    elif it_type == "TE":
        res = run_ivl.run_te(it_opts, cfg)

    elif it_type == "kafka":
        res = run_ivl.run_kafka(it_opts, cfg, False, False)

    else:
        raise invalidtesttype(it_key, it_type)

    return res


def print_header(cfg: dict, files: list):
    '''print all the header information. '''
    # this returns 13 or similar
    ivl_version = run_ivl.get_ivl_version(cfg['suffix'])

    print("running ", end='')
    if cfg['vlog95']:
        print("vlog95 ", end='')
    print("compiler/kafka tests for icarus verilog ", end='')
    # pylint: disable-next=consider-using-f-string
    print("version: {ver}".format(ver=ivl_version), end='')
    if cfg['suffix']:
        # pylint: disable-next=consider-using-f-string
        print(", suffix: {suffix}".format(suffix=cfg['suffix']), end='')
    if cfg['strict']:
        if cfg['force-sv']:
            print(" (strict, force sv)", end='')
        else:
            print(" (strict)", end='')
    elif cfg['force-sv']:
        print(" (force sv)", end='')
    if cfg['with-valgrind']:
        print(" (valgrind)", end='')
    print("")
    # pylint: disable-next=consider-using-f-string
    print("using list(s): {files}".format(files=', '.join(files)))
    print("-" * 76)


if __name__ == "__main__":
    argp = argparse.ArgumentParser(description='')
    argp.add_argument('--suffix', type=str, default='',
                      help='the icarus executable suffix, default "%(default)s".')
    argp.add_argument('--strict', action='store_true',
                      help='force strict standard compliance, default "%(default)s".')
    argp.add_argument('--with-valgrind', action='store_true',
                      help='run the test suite with valgrind, default "%(default)s".')
    argp.add_argument('--force-sv', action='store_true',
                      help='force tests to be run as systemverilog, default "%(default)s".')
    argp.add_argument('--vlog95', action='store_true',
                      help='convert tests to verilog 95 and then run, default "%(default)s".')
    argp.add_argument('files', nargs='*', type=str, default=['regress-kafka.list'],
                      help='file(s) containing a list of the tests to run, default "%(default)s".')
    args = argp.parse_args()

    ivl_cfg = {
        'suffix'        : args.suffix,
        'strict'        : args.strict,
        'with-valgrind' : args.with_valgrind,
        'force-sv'      : args.force_sv,
        'vlog95'        : args.vlog95
              }

    print_header(ivl_cfg, args.files)

    # read the list files, to get the tests.
    tests_list = test_lists.read_lists(args.files)

    # we need the width of the widest key so that we can figure out
    # how to align the key:result columns.
    # pylint: disable-next=invalid-name
    width = max(len(item[0]) for item in tests_list)

    # pylint: disable-next=invalid-name
    error_count = 0
    for cur in tests_list:
        result = process_test(cur, ivl_cfg)
        error_count += result[0]
        # pylint: disable-next=consider-using-f-string
        print("{name:>{width}}: {result}".format(name=cur[0], width=width, result=result[1]))

    print("=" * 76)
    # pylint: disable-next=consider-using-f-string
    print("test results: ran {ran}, failed {failed}.".format(ran=len(tests_list), \
                                                             failed=error_count))
    sys.exit(error_count)

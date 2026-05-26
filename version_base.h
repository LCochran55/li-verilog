#ifndef VERSION
/*
 * This file is generated from version_base.h.in by the build system
 *
 * WARNING! All changes made in this file will be lost when recompiling!
 */
#define VERSION_MAJOR   14
#define VERSION_MINOR   0

/*
 * This will be appended to the version. Use this to mark development
 * versions and the like.
 */
# define VERSION_EXTRA   " (devel)"

# define VERSION_STRINGIFY(x) #x
# define VERSION_STR(a,b,extra) VERSION_STRINGIFY(a.b) extra

#define VERSION VERSION_STR(VERSION_MAJOR,VERSION_MINOR,VERSION_EXTRA)
#endif

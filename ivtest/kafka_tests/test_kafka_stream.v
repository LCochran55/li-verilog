module counter(out, clk, rst);
  output reg [3:0] out;
  input            clk;
  input            rst;

  always @(posedge clk)
      out <= rst ? out + 1 : 0;

endmodule


module test;
  reg        rst = 0;
  reg        clk = 0;
  wire [3:0] val;
  
  initial begin
     $dumpfile("test.vcd");
     $dumpvars(0,test);
     # 5 rst = 1;
     # 5 $finish;
  end
  
  always #1 clk = !clk;
  
  counter c1 (val, clk, rst);
  
endmodule

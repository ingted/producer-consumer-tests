``` ini

BenchmarkDotNet=v0.11.5, OS=ubuntu 19.04
Intel Core i7-6700HQ CPU 2.60GHz (Skylake), 1 CPU, 8 logical and 4 physical cores
.NET Core SDK=2.2.301
  [Host]     : .NET Core 2.2.6 (CoreCLR 4.6.27817.03, CoreFX 4.6.27818.02), 64bit RyuJIT DEBUG
  DefaultJob : .NET Core 2.2.6 (CoreCLR 4.6.27817.03, CoreFX 4.6.27818.02), 64bit RyuJIT


```
|          Method |     Mean |    Error |   StdDev |   Median |
|---------------- |---------:|---------:|---------:|---------:|
| ProdcerConsumer | 253.5 ms | 7.066 ms | 20.83 ms | 243.5 ms |

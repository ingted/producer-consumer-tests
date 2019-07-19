// Learn more about F# at http://fsharp.org

open System
open Akka.FSharp
open Akka.Routing
open Akka.Streams
open Akka.Streams.Dsl
open Akka
open BenchmarkDotNet.Attributes;
open BenchmarkDotNet.Running;

type Product = { Id: int }

let havyWeightPrint i =
    printfn "%d" i
    fun prod -> 
        async {
            do! Async.Sleep (100)
            //printfn "Print %A" prod
            return Ok(prod)
        }

let producer  q =
    async {
        return Seq.unfold(fun x -> Some(x, { x with Id = x.Id + 1})) { Id = 0 } |> Seq.take q
    }



let graph (workers: int) = 
    GraphDsl.Create(fun builder -> 
                        let balancer = builder.Add(Balance<Product>(workers))
                        let mergeResult = builder.Add(new Merge<Result<Product, string>>(workers))
                        let flows = [0..(workers - 1)] |> Seq.map(fun x -> havyWeightPrint x) |> Seq.map(fun f -> Flow.Create<Product>().SelectAsync(50, fun x -> f x |> Async.StartAsTask )) |> Seq.toArray
                        for i in [0..(workers - 1)] do
                            builder.From(balancer.Out(i)).Via(flows.[i].Async()).To(mergeResult.In(i)) |> ignore
                        
                        FlowShape<Product, Result<Product, string>>(balancer.In, mergeResult.Out)
                   )

let printRes res =
    match res with
    | Error err -> 
        printfn "Error"
    | Ok(x) -> 
        printfn "ok(%A)" x

type Bench() =
    let system = System.create "my-system" (Configuration.defaultConfig())
    [<Benchmark>]
    member this.ProdcerConsumer() =
        let product = producer(1600) |> Async.RunSynchronously
        Source.From(product).Via(graph(50)).RunWith(Sink.Ignore(), system.Materializer()) |> Async.AwaitTask |> Async.RunSynchronously        

[<EntryPoint>]
let main argv =
    let sumamry = BenchmarkRunner.Run<Bench>()
    printfn "Hello World from F#! %A" sumamry
    0 // return an integer exit code

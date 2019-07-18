// Learn more about F# at http://fsharp.org

open System
open Akka.FSharp
open Akka.Routing

type Product = { Id: int }

type ConsumerCommand = 
    | PrintProduct of product: Product
    | ProductPrinted
    | ProductionCompleted

let havyWeightPrint prod =
    async {
        do! Async.Sleep (10)
        printfn "Print %A" prod
        return ProductPrinted
    }

let producer q =
    async {
        return Seq.unfold(fun x -> Some(x, { x with Id = x.Id + 1})) { Id = 0 } |> Seq.take q
    }


type ConsumerState = { all: int; completed: int; productionDone: bool } 

let consumer (mailbox: Actor<_>) =
    let rec loop () = actor {
        match! mailbox.Receive () with
        | PrintProduct prod -> 
            havyWeightPrint prod |!>  mailbox.Sender()
            return! loop ()
        | _ -> 
            printfn "Not for me message"
            return! loop ()
    }
    loop ()

let consumerCoordinator (mailbox: Actor<_>) = 
    let actors = spawnOpt mailbox.Context.System "consumer" (consumer) ([SpawnOption.Router(RoundRobinPool(Environment.ProcessorCount))])
    let rec loop (state) = actor {
        match! mailbox.Receive () with
        | PrintProduct p ->
            actors <! PrintProduct(p)
            return! loop ({ state with all = state.all + 1})
        | ProductPrinted ->
            let newState = { state with completed = state.completed + 1}
            printfn "Completed %A" newState 
            if state.productionDone then
                if state.all = newState.completed then 
                    printfn "Zegnajcie"
                    return "End"
                else
                    return! loop (newState)
            else 
                return! loop (newState)
        | ProductionCompleted -> 
            return! loop ({state with productionDone = true})
        return! loop (state)
    }
    loop ({ all = 0; completed = 0; productionDone = false} )

[<EntryPoint>]
let main argv =
    use system = System.create "my-system" (Configuration.defaultConfig())
    let consumer = spawn system "consumer-coordinator" consumerCoordinator
    let product = producer(16000000) |> Async.RunSynchronously
    for x in product do 
        consumer <! PrintProduct(x)
    consumer <! ProductionCompleted
    Console.ReadKey();
    printfn "Hello World from F#! %A" product
    0 // return an integer exit code

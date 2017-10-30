
#r "packages/SQLProvider/lib/net451/FSharp.Data.SQLProvider.dll"

open System.IO
open FSharp.Data.Sql

let readLines (filePath:string) = seq {
    use sr = new StreamReader (filePath)
    while not sr.EndOfStream do
        yield sr.ReadLine ()
}
;;

type LogRec = {
    Date: string
    Time: string
    LogLevel: string
    Reporter: string
    Message: string
}

let [<Literal>] dbVendor = Common.DatabaseProviderTypes.MSSQLSERVER
let [<Literal>] connString = "server=DESKTOP-J26QK31;database=LogAnalize;user id=sa;password='superadmin'"
let [<Literal>] connStringName = "DefaultConnectionString"
let [<Literal>] indivAmount = 1000
let [<Literal>] useOptTypes  = true

type sql =
    SqlDataProvider<
        dbVendor,
        connString,
        IndividualsAmount = indivAmount,
        UseOptionTypes = useOptTypes>
let ctx = sql.GetDataContext()

//ctx.Dbo.Log.Individuals


//let test = readLines "UmbracoTraceLog/UmbracoTraceLog.txt.2017-09-01"
let regex = new System.Text.RegularExpressions.Regex("\[Thread \d+\]")

let readLogRecFromLine (line: string) =
    let fn (f: string) s = 
        match f.Split(' ') with
        | [| a; b; c; d; e; f; g; h |] ->
            Some {
                Date = a
                Time = b
                LogLevel = d
                Reporter = f
                Message = s
            }
        | _ -> None        
    regex.Split(line) |> function 
    | [|first ; second|] -> fn first second
    | _ -> None

type ParseResult =
    | LogRec of LogRec
    | Raw of string

let parseLogFile (filePath: string) =
    readLines filePath
    |> Seq.map (fun line -> 
        readLogRecFromLine line
        |> function
            | Some logRec -> LogRec logRec
            | None -> Raw line)


let normalize (prev: LogRec option) (item: ParseResult) =
    match prev, item with
    | _, LogRec lrec -> false, lrec
    | Some prevRec, Raw str -> 
        let headText = prevRec.Message
        true, { prevRec with Message = (headText + str) }
    | _ -> failwith "wrong format"

let rec normalizeResults list prev (results: ParseResult list) =
    match results with
    | head :: tail ->
        normalize prev head |> function
        | false, lrec -> 
            let list' = 
                match prev with
                | Some prevRec -> prevRec :: list
                | None -> list
            normalizeResults list' (Some lrec) tail
        | true, lrec ->
            normalizeResults list (Some lrec) tail
    | [] -> 
        match prev with
        | Some lrec -> lrec :: list
        | None -> list

let normalizeLogs path =
    parseLogFile path
    |> Seq.toList
    |> normalizeResults [] None

let unwrap result =
    match result with
    | LogRec lrec -> lrec
    | _ -> failwith "not handled"


let storeRec (lrec: LogRec) =
    let row = ctx.Dbo.Log.Create()
    row.LogTime <- Some (System.DateTime.Parse(lrec.Date + " " + lrec.Time.Replace(',', '.')))
    row.LogLevel <- Some(lrec.LogLevel)
    row.Reporter <- Some(lrec.Reporter)
    row.Message <- Some(lrec.Message)
    ctx.SubmitUpdates() |> ignore

normalizeLogs "UmbracoTraceLog/UmbracoTraceLog.txt.2017-09-01" 
|> List.filter (fun r -> r.Message.Contains("logged in")) 
|> List.map storeRec
|> ignore



let storeLogs path =
    normalizeLogs path
    |> Seq.filter (fun r -> r.Message.Contains("logged in")) //|> Seq.length
    |> Seq.map storeRec
    |> Seq.length

let subpacket size list =
    let rec take size packet list =
        match size, list with
        | 0, _ | _, [] -> (Seq.rev packet), list
        | _, head::tail -> 
            take (size - 1) (head::packet) tail

    seq {
        let mutable list' = list
        if Seq.isEmpty list' then yield! Seq.empty
        else
            while (not <| Seq.isEmpty list') do
                let p, rest = take size [] list'
                list' <- rest
                yield p        
    }

let bulkInsert recs =
    let ctx = sql.GetDataContext()
    let mapToRow (logRec: LogRec) =
        let row = ctx.Dbo.Log.Create()
        row.LogTime <- Some (System.DateTime.Parse(logRec.Date + " " + logRec.Time.Replace(',', '.')))
        row.LogLevel <- Some(logRec.LogLevel)
        row.Reporter <- Some(logRec.Reporter)
        row.Message <- Some(logRec.Message)
        row

    Seq.iter (mapToRow >> ignore) recs
    ctx.SubmitUpdates()


// let sample = "2017-09-01 00:21:54,136 [15] WARN  Umbraco.Web.Routing.DefaultUrlProvider - [Thread 10] Couldn't find any page with nodeId=31265. This is most likely caused by the page not being published."

// regex.Split(sample)

// readLogRecFromLine sample

// let temp = parseLogFile "UmbracoTraceLog/UmbracoTraceLog.txt.2017-09-01" |> Seq.item 8

// normalize (Some (unwrap temp)) (Raw "hehe")

storeLogs "UmbracoTraceLog/UmbracoTraceLog.txt.2017-09-01"

let sampleRecs = normalizeLogs "UmbracoTraceLog/UmbracoTraceLog.txt.2017-09-01" 
sampleRecs |> List.take 2
    |> List.map (fun (logRec: LogRec) -> 
        let row = ctx.Dbo.Log.Create()
        row.LogTime <- Some (System.DateTime.Parse(logRec.Date + " " + logRec.Time.Replace(',', '.')))
        row.LogLevel <- Some(logRec.LogLevel)
        row.Reporter <- Some(logRec.Reporter)
        row.Message <- Some(logRec.Message)
        row)
    |> (fun _ -> ctx.SubmitUpdates())

normalizeLogs "UmbracoTraceLog/UmbracoTraceLog.txt.2017-09-01" 
|> subpacket 100 
|> Seq.iter bulkInsert

let pathes = seq {
    yield! (
        [1..30] 
        |> List.map (fun n -> "UmbracoTraceLog/UmbracoTraceLog.txt.2017-09-" + n.ToString().PadLeft(2, '0')))
    yield! (
        [1..26] 
        |> List.map (fun n -> "UmbracoTraceLog/UmbracoTraceLog.txt.2017-10-" + n.ToString().PadLeft(2, '0')))
} 
pathes
|> (fun pathes -> seq { 
        for p in pathes do 
            yield! (normalizeLogs p) })
|> Seq.toList
|> subpacket 100
|> Seq.iter bulkInsert
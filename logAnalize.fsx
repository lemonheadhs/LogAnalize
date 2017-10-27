
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
let sample = "2017-09-01 00:21:54,136 [15] WARN  Umbraco.Web.Routing.DefaultUrlProvider - [Thread 10] Couldn't find any page with nodeId=31265. This is most likely caused by the page not being published."
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

regex.Split(sample)

readLogRecFromLine sample

let temp = parseLogFile "UmbracoTraceLog/UmbracoTraceLog.txt.2017-09-01" |> Seq.item 8
let normalize (prev: LogRec option) (item: ParseResult) =
    match prev, item with
    | _, LogRec lrec -> false, lrec
    | Some prevRec, Raw str -> 
        let headText = prevRec.Message
        true, { prevRec with Message = (headText + str) }
    | _ -> failwith "wrong format"

let normalizeResults results =
    

let unwrap result =
    match result with
    | LogRec lrec -> lrec
    | _ -> failwith "not handled"

normalize (Some (unwrap temp)) (Raw "hehe")

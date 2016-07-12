(* TODO

1) filters and maps across multiple columns...
   this might not be possible without figuring out the
   typing BS i'm dealing with
2) hierarchical dataframe types
3) group by
4) add option type to parser (malformed or missing values)
5) better row representation
6) pivots (cf. group by)
7) date col type
8) boolean col type
9) object-oriented interface e.g. df#float_col "ic50"
10) to_csv
11) pretty-printing a dataframe
12) sum(), mean(), median(), mode(), quantiles(), min(), max()
13) frequency map (this is a group by + sum over the groups)
14) rename index to mask, and add index (take list of indices to pull from column/table)
*)


open Nonstd

open Common

module Spec = struct
  type column_name = string

  type column_type =
    | Float
    | Int
    | String

  type column =
    | Column of column_name * column_type

  type t = column list

  let float = Float
  let int = Int
  let string = String

  let column_name column_spec =
    match column_spec with
    | Column (name, _) -> name

  let of_list l =
    List.map l ~f:(fun (name, kind) -> Column (name, kind))

  (** Subsets spec to only have columns with names in colnames. *)
  let subset spec colnames =
    List.map spec ~f:(fun colspec ->
        match colspec with
        | Column (name, coltype) as col ->
          if List.mem name ~set:colnames then Some col else None)
    |> List.filter_opt
end


module Parser = struct
  type column_parser =
    | FloatParser of (string -> float)
    | IntParser of (string -> int)
    | StringParser of (string -> string)

  let get_parser column_spec =
    match column_spec with
    | Spec.Column (name, coltype) ->
    begin match coltype with
    | Spec.Float ->
      FloatParser
        (fun x -> Float.of_string x
                  |> Option.value_exn ~msg:"Not a valid float")
    | Spec.Int ->
      IntParser
        (fun x -> Int.of_string x
                  |> Option.value_exn ~msg:"Not a valid int")
    | Spec.String -> StringParser (fun x -> x)
    end
end


module Dataframe = struct
  open Result

  type column_name = string

  type row_value =
    | Float of float
    | Int of int
    | String of string

  type column_values =
    | FloatColumn of float list
    | IntColumn of int list
    | StringColumn of string list
  (* | CategoryColumn of string list *)
  (* | DateColumn of string list *)
  (* | CharColumn of char list  *)

  type column = (column_name * column_values)

  type t = {
    spec: Spec.t;
    data: column list
  }

  let parse_column_for_spec column_spec raw_column =
    let pars = Parser.get_parser column_spec in
    let column_name = Spec.column_name column_spec in
    let parse_column pars vals =
      match pars with
      | Parser.FloatParser pars -> FloatColumn (List.map ~f:pars vals)
      | Parser.IntParser pars -> IntColumn (List.map ~f:pars vals)
      | Parser.StringParser pars -> StringColumn (List.map ~f:pars vals)
    in
    let values = parse_column pars raw_column in
    (column_name, values)

  let of_csv ?header ~spec filename : t =
    let columns =
      open_in filename
      |> Csv.of_channel
      |> Csv.input_all
      |> transpose
    in
    {spec; data = List.map2 spec columns ~f:parse_column_for_spec}

  let get_col {data;} name : (column, string) result =
    match List.find data ~f:(fun (n, _) -> n = name) with
    | Some col -> Ok col
    | None -> Error (sprintf "No column with name %s" name)

  let get_float_col df name =
    get_col df name
    >>= function
    | _, FloatColumn values -> return values
    | _ -> err (sprintf "Column `%s` is not a FloatColumn" name)

  let get_int_col df name =
    get_col df name
    >>= function
    | _, IntColumn values -> return values
    | _ -> err (sprintf "Column `%s` is not an IntColumn" name)

  let get_string_col df name =
    get_col df name
    >>= function
    | _, StringColumn values -> return values
    | _ -> err (sprintf "Column `%s` is not a StringColumn" name)

  (** Get the nth row of the df. *)
  let nth {data;} n =
    (* TODO: better to use exceptions or continuations to short-circuit this: *)
    List.map data ~f:(fun (colname, values) ->
        match values with
        | FloatColumn fs ->
          begin match List.findi fs ~f:(fun i v -> i = n) with
          | None -> Error "No %s row of df"
          | Some (_, v) -> Ok (colname, (Float v))
          end
        | IntColumn is ->
          begin match List.findi is ~f:(fun i v -> i = n) with
          | None -> Error "No %s row of df"
          | Some (_, v) -> Ok (colname, (Int v))
          end
        | StringColumn ss ->
          begin match List.findi ss ~f:(fun i v -> i = n) with
          | None -> Error "No %s row of df"
          | Some (_, v) -> Ok (colname, (String v))
          end)
    |> List.fold ~init:(Ok []) ~f:(fun res r ->
        res >>= (fun acc -> r >>= function v -> return (v::acc)))

  let get_name (column:column) : column_name =
    let name, _ = column in
    name

  let replace_col {data; spec} colname newcol =
    {spec;
     data =
       List.map data ~f:(fun (name, vals) ->
           if name = get_name newcol
           then newcol
           else (name, vals))}

  let err_bad_col_type name coltype =
    err (sprintf "Column %s is not a %s" name coltype)

  (** Map the function across the specified column, returning the dataframe with
      the resulting column. *)
  let map_float_column df colname f =
    get_col df colname
    >>= (function
    | name, FloatColumn vals -> return (name, FloatColumn (List.map ~f vals))
    | _ -> err_bad_col_type colname "FloatColumn")
    >>= fun newcol ->
    return (replace_col df colname newcol)

  let map_int_column df colname f =
    get_col df colname
    >>= (function
    | name, IntColumn vals -> return (name, IntColumn (List.map ~f vals))
    | _ -> err_bad_col_type colname "IntColumn")
    >>= fun newcol ->
    return (replace_col df colname newcol)

  let map_string_column df colname f =
    get_col df colname
    >>= (function
    | name, StringColumn vals -> return (name, StringColumn (List.map ~f vals))
    | _ -> err_bad_col_type colname "StringColumn")
    >>= fun newcol ->
    return (replace_col df colname newcol)

  let index_list idx vals =
    (* TODO: this can exn; should make this safe. *)
    (List.map2 idx vals
       ~f:(fun i v -> if i then Some v else None)
     |> List.filter_opt)

  (** Index the column by the boolean mask `idx'. *)
  let index_column (column:column) idx : column =
    match column with
    | name, FloatColumn vals -> name, FloatColumn (index_list idx vals)
    | name, IntColumn vals -> name, IntColumn (index_list idx vals)
    | name, StringColumn vals -> name, StringColumn (index_list idx vals)

  (** Index the dataframe by the boolean mask `idx'. *)
  let index {spec; data} idx : t =
    {spec;
     data = List.map ~f:(fun col -> index_column col idx) data}

  (** Return dataframe with only the specified columns. *)
  let subset ({spec; data} as df) colnames =
    List.map colnames ~f:(fun name -> get_col df name)
    |> collapse
    >>= fun new_columns ->
    return {spec = Spec.subset spec colnames;
            data = new_columns}

  (** Filter rows of df by applying f to the specified column; `true' retains
      the row, else `false' drops it from the resulting `Dataframe.t'. *)
  let filter_by_float_column df colname f =
    get_col df colname
    >>= (function
    | _, FloatColumn vals ->
      return (List.map ~f vals)
    | _ -> err (sprintf "Column `%s` is not a FloatColumn" colname))
    >>= fun idx ->
    return (index df idx)

  let filter_by_int_column df colname f =
    get_col df colname
    >>= (function
    | _, IntColumn vals ->
      return (List.map ~f vals)
    | _ -> err (sprintf "Column `%s` is not a IntColumn" colname))
    >>= fun idx ->
    return (index df idx)

  let filter_by_string_column df colname f =
    get_col df colname
    >>= (function
    | _, StringColumn vals ->
      return (List.map ~f vals)
    | _ -> err (sprintf "Column `%s` is not a StringColumn" colname))
    >>= fun idx ->
    return (index df idx)

  let exnify1 fn x = match fn x with | Ok v -> v | Error m -> failwith m
  let exnify2 fn x y = match fn x y with | Ok v -> v | Error m -> failwith m
  let exnify3 fn x y z = match fn x y z with | Ok v -> v | Error m -> failwith m

  let get_float_col_exn = exnify2 get_float_col
  let get_int_col_exn = exnify2 get_int_col
  let get_string_col_exn = exnify2 get_string_col
  let nth_exn = exnify2 nth
end


let hla_types =
  let spec =
    let open Spec in
    Spec.of_list
      ["type", string;
       "cat", string;
       "count", int]
  in
  Dataframe.of_csv ~spec "/Users/isaachodes/Desktop/test.csv"

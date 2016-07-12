(* PPX-object idea

   Because adding new columns to a record-based dataframe is too hard/impossible to
   do nicely, whereas it's easy to do with objects (via inheriting the old df).

*)

open Nonstd


let get_csv_rows filename =
  let ic = open_in filename in
  let rows =
    Csv.of_channel ic
    |> Csv.input_all
  in
  close_in ic;
  rows


module Spec = struct
  type t =  P
end


module Dataframe = struct
  let of_csv ~spec filename =
    let rows = get_csv_rows filename in
    List.mapi ~f:spec rows
end

let custom parser_ name = Spec.P
let int name = Spec.P
let float name = Spec.P
let string name = Spec.P

let ( ++ ) a b = (a, b)


module Allele = struct type t = int  let of_string s : t option = Some 1 end

(* Define what a row looks like your dataframe *)
type hla = { allele_type : Allele.t; (* This module must define
                                             of_string : string -> Allele.t *)
             certainty : float;
             count : int;
             note : string option } [@@deriving dataframe]
(* The above will generate: *)
type hla_row =
  < allele_type : Allele.t;
    certainty : float;
    count : int;
    note : string option >
type hla_df =
  < allele_types : Allele.t list;
    certainties : float list;
    counts : int list;
    notes : string list >

let hla_spec idx (row:string list) : hla_row =
  (* This would obviously have better error handling... *)
  let unwrap m pos =
    Option.value_exn
      ~msg:(sprintf "Expected a %s at position %d on line %d" m pos idx)
  in
  object
    method allele_type = Allele.of_string (List.nth row 0) |> unwrap "Allele" 0
    method certainty = Float.of_string (List.nth_exn row 1) |> unwrap "Float" 1
    method count = Int.of_string (List.nth_exn row 2) |> unwrap "Int" 2
    method note = List.nth row 3
  end

(* example use *)
let df = Dataframe.of_csv ~spec:hla_spec "/Users/isaachodes/Desktop/test.csv"

(* which would return a dataframe of type hla_type_df, looking like:
   {
     allele_types = [...];
     ...
   }

  Could also use streams instead of lists, here.
*)

let add_column df vals = object
  method foo = vals
end [@@dataframe incl df]

let remove_column (df:< .. >) colname =
  df [@@dataframe remove_column colname]


let df = remove_column df "counts"

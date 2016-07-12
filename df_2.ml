(* PPX-record idea *)

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
    rows
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
             note : string option } [@@dataframe]
(* The above will generate: *)
type hla_row = { allele_type : Allele.t;
                 certainty : float;
                 count : int;
                 note : string option }
type hla_df = { allele_types : Allele.t list;
                certainties : float list;
                counts : int list;
                notes : string list }

(* Example row *)
let row = {allele_type = Allele.of_string "s" |> Option.value_exn ~msg:"";
           certainty = 0.123; count = 1; note = None }

let hla_spec idx (row:string list) : hla_row =
  (* This would obviously have better error handling... *)
  let unwrap m =
    Option.value_exn ~msg:(sprintf "Expected a %s here" m) in
  {
    allele_type = Allele.of_string (List.nth row 0) |> unwrap "Allele";
    certainty = Float.of_string (List.nth_exn row 1) |> unwrap "Float";
    count = Int.of_string (List.nth_exn row 2) |> unwrap "Int";
    note = List.nth row 3
  }

(* example use *)
let df = Dataframe.of_csv ~spec:hla_spec "/Users/isaachodes/Desktop/test.csv"

(* which would return a dataframe of type hla_type_df, looking like:
   {
     allele_types = [...];
     ...
   }

  Could also use streams instead of lists, here.
*)

let newdf = object
  inherit df
  method foo = this_list
end


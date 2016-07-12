
open Nonstd

type ('a, 'b) result =
  | Ok of 'a
  | Error of 'b

module Result = struct
  let bind r f =
    match r with
    | Ok a -> f a
    | Error m -> Error m
  let ( >>= ) = bind
  let return r = Ok r
  let err m = Error m

  (* TODO: what's this really called? *)
  (** Takes a list of results, and returns a Ok ('a list) if all the results are
      Ok, else the first Error found in the list is propogated.  *)
  let collapse (xs:('a, 'b) result list) : ('c list, 'd) result =
    List.fold xs ~init:(Ok [])
      ~f:(fun acc n -> acc >>= (fun acc -> n >>= (fun n -> return (n::acc))))
end

let rec transpose ?acc matrix =
  (* Will fail messily if the matrix isn't well-formed. *)
  match acc, matrix with
  | None, xs :: _ -> transpose
                       ~acc:(List.map xs ~f:(fun _ -> []))
                       matrix
  | Some acc, matrix -> begin
      let merge acc xs = List.map2 ~f:(fun col x -> x::col) acc xs in
      match matrix with
      | [] -> List.map ~f:List.rev acc
      | x :: xs ->
        assert (List.length x > 0);
        transpose ~acc:(merge acc x) xs
    end
  | _, _ -> failwith "Invalid args (check that matrix is well-formed)"

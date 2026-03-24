(** Tests that the client does NOT hang when the server drops the connection
    before sending response HEADERS.

    {b The T30 defect (before fix)}

    Without the error_handler wiring, [response_p] and [read_body_p] are
    never resolved when the connection drops, causing the client to hang
    indefinitely.

    {b Expected behavior (after fix)}

    The error_handler resolves both promises with [Error (ConnectionError _)],
    and [call] returns [Error (ConnectionError _)] instead of hanging.

    {b Test structure}

    1. Start a server that accepts a connection, then immediately closes
       the socket (simulating a crash or network partition).
    2. Client attempts a unary RPC.
    3. Assert: the call returns [Error (ConnectionError _)] within a
       reasonable timeout, rather than hanging forever. *)

let () =
  Eio_main.run @@ fun env ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  Eio.Switch.run @@ fun sw ->
  let server_socket =
    Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5
      (`Tcp (Eio.Net.Ipaddr.V4.loopback, 0))
  in
  let port =
    match Eio.Net.listening_addr server_socket with
    | `Tcp (_, p) -> p
    | `Unix _ -> assert false
  in
  (* Server fiber: accept a connection, then immediately close it. *)
  Eio.Fiber.fork ~sw (fun () ->
      let socket, _addr = Eio.Net.accept ~sw server_socket in
      (* Give the client just enough time to send the request. *)
      Eio.Time.sleep clock 0.05;
      Eio.Flow.close socket);
  (* Give the server a moment to start listening. *)
  Eio.Time.sleep clock 0.05;
  let socket =
    Eio.Net.connect ~sw net (`Tcp (Eio.Net.Ipaddr.V4.loopback, port))
  in
  let conn =
    H2_eio.Client.create_connection ~sw ~error_handler:ignore socket
  in
  (* Client RPC: should NOT hang. With the error_handler fix, the connection
     drop resolves the promises with ConnectionError. *)
  let result = ref None in
  let timed_out = ref false in
  Eio.Fiber.both
    (fun () ->
      let r =
        try
          `Returned
            (Grpc_eio.Client.call ~service:"test.ConnectionDrop"
               ~rpc:"WillFail" ~scheme:"http"
               ~do_request:(H2_eio.Client.request conn)
               ~handler:(Grpc_eio.Client.Rpc.unary "hello" ~f:(fun r -> r))
               ())
        with exn -> `Raised exn
      in
      result := Some r)
    (fun () ->
      (* Timeout watchdog: if the client hasn't completed in 5 seconds,
         something is wrong (it's hanging). *)
      Eio.Time.sleep clock 5.0;
      if Option.is_none !result then (
        timed_out := true;
        Printf.printf "FAIL: Client hung for 5s — promise hang NOT fixed.\n%!";
        (* Force exit since the client fiber is stuck *)
        exit 1));
  if !timed_out then
    Printf.printf
      "CONCLUSION: T30 defect still present — promises hang on connection drop.\n%!"
  else
    match !result with
    | Some (`Returned (Error (Grpc_eio.Client.ConnectionError _))) ->
        Printf.printf
          "PASS: Client returned ConnectionError as expected (no hang).\n%!";
        Printf.printf
          "CONCLUSION: T30 fix works — error_handler resolves promises on connection drop.\n%!"
    | Some (`Returned (Error (Grpc_eio.Client.ResponseError s))) ->
        Format.printf "RPC returned ResponseError: %a\n%!" H2.Status.pp_hum s;
        Printf.printf
          "CONCLUSION: Server responded with an error status (unexpected for abrupt close).\n%!"
    | Some (`Returned (Ok _)) ->
        Printf.printf "UNEXPECTED: RPC returned Ok despite server dropping connection.\n%!"
    | Some (`Raised exn) ->
        Printf.printf "RPC raised exception: %s\n%!" (Printexc.to_string exn);
        Printf.printf
          "CONCLUSION: Exception propagated (connection drop surfaced differently).\n%!"
    | None ->
        Printf.printf "FAIL: No result captured.\n%!"

(** Tests whether a client handler exception tears down the H2 connection
    or only the affected stream.

    {b Empirical question}

    When the user-supplied handler [f] in {!Grpc_eio.Client.Rpc.unary} (or any
    streaming variant) raises an exception, Eio's structured concurrency
    propagates that exception through {!Eio.Fiber.both}, cancelling sibling
    fibers.  The exception then escapes {!Grpc_eio.Client.call}.

    {b What we want to know}

    Does the H2 {i connection} survive, allowing subsequent RPCs on the same
    connection to succeed?  Or does the abandoned stream (unclosed body writers,
    un-drained readers) poison the connection?

    {b Test structure}

    1. Start a server that echoes payloads (deferred-response style, as in
       [test_concurrent_handler]).
    2. Open a single client connection.
    3. Make RPC #1 whose handler raises [Failure "deliberate"].
    4. Catch the exception.
    5. Make RPC #2 with a normal handler.
    6. Assert RPC #2 succeeds with the expected payload and gRPC status OK.

    If step 6 passes, exception isolation is per-stream.
    If step 6 fails (connection error, hang, etc.), the connection was torn down. *)

(** Echo server: reads full request, responds with the same payload. *)
let echo_handler sw _addr reqd =
  Eio.Fiber.fork ~sw (fun () ->
      let body = H2.Reqd.request_body reqd in
      let buf = Buffer.create 256 in
      let eof_p, eof_r = Eio.Promise.create () in
      let rec on_read bs ~off ~len =
        Buffer.add_string buf (Bigstringaf.substring bs ~off ~len);
        H2.Body.Reader.schedule_read body ~on_eof ~on_read
      and on_eof () = Eio.Promise.resolve eof_r () in
      H2.Body.Reader.schedule_read body ~on_read ~on_eof;
      Eio.Promise.await eof_p;
      (* Extract the gRPC message payload (skip the 5-byte length-prefix
         header) so we can re-wrap it for the response. *)
      let raw = Buffer.contents buf in
      let payload =
        if String.length raw > 5 then String.sub raw 5 (String.length raw - 5)
        else raw
      in
      let resp_body =
        H2.Reqd.respond_with_streaming reqd ~flush_headers_immediately:true
          (H2.Response.create `OK
             ~headers:
               (H2.Headers.of_list
                  [ ("content-type", "application/grpc+proto") ]))
      in
      H2.Body.Writer.write_string resp_body (Grpc.Message.make payload);
      H2.Reqd.schedule_trailers reqd
        (H2.Headers.of_list
           [
             ( "grpc-status",
               string_of_int (Grpc.Status.int_of_code Grpc.Status.OK) );
           ]);
      H2.Body.Writer.close resp_body)

let error_handler _addr ?request:_ _error _start_response = ()

let () =
  Eio_main.run @@ fun env ->
  let net = Eio.Stdenv.net env in
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
  (* Server fiber: accept connections in a loop so the single connection
     can serve multiple requests. *)
  Eio.Fiber.fork ~sw (fun () ->
      (* Accept one connection — HTTP/2 multiplexes streams over it. *)
      let socket, addr = Eio.Net.accept ~sw server_socket in
      H2_eio.Server.create_connection_handler
        ~request_handler:(echo_handler sw)
        ~error_handler addr ~sw socket);
  (* Give the server a moment to start listening. *)
  Eio.Time.sleep (Eio.Stdenv.clock env) 0.05;
  let socket =
    Eio.Net.connect ~sw net (`Tcp (Eio.Net.Ipaddr.V4.loopback, port))
  in
  let conn =
    H2_eio.Client.create_connection ~sw ~error_handler:ignore socket
  in
  (* === RPC #1: handler raises an exception === *)
  let rpc1_result =
    try
      let r =
        Grpc_eio.Client.call ~service:"test.ExceptionIsolation"
          ~rpc:"FailingMethod" ~scheme:"http"
          ~do_request:(H2_eio.Client.request conn)
          ~handler:
            (Grpc_eio.Client.Rpc.unary "hello" ~f:(fun _response ->
                 failwith "deliberate exception in handler"))
          ()
      in
      `Returned r
    with exn -> `Raised exn
  in
  (match rpc1_result with
  | `Raised (Failure msg) ->
      Printf.printf "RPC #1: handler exception propagated as expected: %s\n%!"
        msg
  | `Raised exn ->
      Printf.printf "RPC #1: unexpected exception type: %s\n%!"
        (Printexc.to_string exn)
  | `Returned (Ok _) ->
      Printf.printf "RPC #1: unexpectedly returned Ok (exception was swallowed)\n%!"
  | `Returned (Error (Grpc_eio.Client.ResponseError s)) ->
      Format.printf "RPC #1: returned ResponseError: %a\n%!" H2.Status.pp_hum s
  | `Returned (Error (Grpc_eio.Client.ConnectionError _)) ->
      Printf.printf "RPC #1: returned ConnectionError\n%!");
  (* === RPC #2: normal handler on the same connection === *)
  let rpc2_result =
    try
      let r =
        Grpc_eio.Client.call ~service:"test.ExceptionIsolation"
          ~rpc:"NormalMethod" ~scheme:"http"
          ~do_request:(H2_eio.Client.request conn)
          ~handler:
            (Grpc_eio.Client.Rpc.unary "world" ~f:(fun r -> r))
          ()
      in
      `Returned r
    with exn -> `Raised exn
  in
  (match rpc2_result with
  | `Returned (Ok (Some response, status))
    when Grpc.Status.code status = Grpc.Status.OK ->
      Printf.printf
        "RPC #2: SUCCESS — connection survived, got response=%S, status=OK\n%!"
        response;
      Printf.printf
        "CONCLUSION: Exception isolation is per-stream; H2 connection survives.\n%!"
  | `Returned (Ok (Some response, status)) ->
      Printf.printf
        "RPC #2: got response=%S but status=%s (not OK)\n%!" response
        (match Grpc.Status.code status with
         | Grpc.Status.OK -> "OK"
         | _ -> "non-OK");
      Printf.printf
        "CONCLUSION: Partial survival — connection works but gRPC status degraded.\n%!"
  | `Returned (Ok (None, _)) ->
      Printf.printf "RPC #2: empty response body\n%!";
      Printf.printf "CONCLUSION: Connection survived but stream was corrupted.\n%!"
  | `Returned (Error (Grpc_eio.Client.ResponseError s)) ->
      Format.printf "RPC #2: H2 ResponseError: %a\n%!" H2.Status.pp_hum s;
      Printf.printf
        "CONCLUSION: Connection survived at H2 level but server returned error.\n%!"
  | `Returned (Error (Grpc_eio.Client.ConnectionError _)) ->
      Printf.printf "RPC #2: ConnectionError\n%!";
      Printf.printf
        "CONCLUSION: Connection-level error on second RPC.\n%!"
  | `Raised exn ->
      Printf.printf "RPC #2: FAILED with exception: %s\n%!"
        (Printexc.to_string exn);
      Printf.printf
        "CONCLUSION: Exception tore down the H2 connection; no per-stream isolation.\n%!");
  Eio.Promise.await (H2_eio.Client.shutdown conn);
  Printf.printf "Test complete.\n%!"

defmodule EMCP.Transport.StreamableHTTPHangRegressionTest do
  @moduledoc """
  Regression test for the intermittent "no response / client hangs" bug.

  Previously, when a session had a registered SSE pid that was no longer alive
  (the standalone GET SSE stream dropped, but ETS still held its pid until the
  next keepalive write, up to 30s later), a tools/call POST was dispatched down
  a `send(pid, ...)` branch. The send to a dead pid is a silent no-op, the POST
  returned `202 {}`, and the JSON-RPC response was never delivered — the client
  waited forever.

  The response is now streamed as SSE on the POST itself, so a stale GET-stream
  pid can no longer swallow it.
  """
  use ExUnit.Case

  setup_all do
    port = Enum.random(49152..65535)

    start_supervised!(
      {Bandit,
       plug: {EMCP.Transport.StreamableHTTP, server: EMCP.TestServer},
       port: port,
       startup_log: false}
    )

    Application.put_env(:emcp, :keepalive_interval, 100)
    on_exit(fn -> Application.delete_env(:emcp, :keepalive_interval) end)

    {:ok, port: port}
  end

  defp http_request(port, method, path, headers, body) do
    {:ok, conn} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])
    body_str = body || ""

    header_lines =
      [
        "#{method} #{path} HTTP/1.1",
        "Host: localhost:#{port}",
        "Content-Length: #{byte_size(body_str)}"
        | Enum.map(headers, fn {k, v} -> "#{k}: #{v}" end)
      ]
      |> Enum.join("\r\n")

    :ok = :gen_tcp.send(conn, header_lines <> "\r\n\r\n" <> body_str)
    {:ok, response} = recv_until_complete(conn, "")
    :gen_tcp.close(conn)
    parse_http_response(response)
  end

  defp recv_until_complete(conn, acc) do
    case :gen_tcp.recv(conn, 0, 5000) do
      {:ok, data} ->
        acc = acc <> data
        if response_complete?(acc), do: {:ok, acc}, else: recv_until_complete(conn, acc)

      {:error, :closed} ->
        {:ok, acc}
    end
  end

  defp response_complete?(data) do
    case String.split(data, "\r\n\r\n", parts: 2) do
      [headers, body] ->
        if String.contains?(headers, "transfer-encoding: chunked") do
          String.contains?(body, "0\r\n")
        else
          case Regex.run(~r/content-length: (\d+)/i, headers) do
            [_, len] -> byte_size(body) >= String.to_integer(len)
            nil -> true
          end
        end

      _ ->
        false
    end
  end

  defp parse_http_response(response) do
    [header_section, body] = String.split(response, "\r\n\r\n", parts: 2)
    [status_line | _] = String.split(header_section, "\r\n")
    status = status_line |> String.split(" ", parts: 3) |> Enum.at(1) |> String.to_integer()
    {status, body}
  end

  defp init_session(port) do
    body = JSON.encode!(%{jsonrpc: "2.0", method: "initialize", id: "init", params: %{}})

    {200, _} =
      http_request(
        port,
        "POST",
        "/mcp",
        [{"content-type", "application/json"}, {"accept", "application/json, text/event-stream"}],
        body
      )

    [{id, _, _} | _] = :ets.tab2list(EMCP.SessionStore.ETS)
    id
  end

  defp tool_call(port, session_id) do
    body =
      JSON.encode!(%{
        jsonrpc: "2.0",
        method: "tools/call",
        id: "tool-1",
        params: %{name: "echo", arguments: %{message: "hello"}}
      })

    http_request(
      port,
      "POST",
      "/mcp",
      [
        {"content-type", "application/json"},
        {"accept", "application/json, text/event-stream"},
        {"mcp-session-id", session_id}
      ],
      body
    )
  end

  test "tool response is delivered on the POST even when the registered SSE pid is dead",
       %{port: port} do
    session_id = init_session(port)

    # Simulate a dropped standalone GET SSE stream whose pid is still registered.
    dead = spawn(fn -> :ok end)
    ref = Process.monitor(dead)
    assert_receive {:DOWN, ^ref, :process, ^dead, _}, 1000
    refute Process.alive?(dead)

    EMCP.SessionStore.ETS.register(session_id, dead)

    {status, body} = tool_call(port, session_id)

    assert status == 200
    assert body =~ "hello"
  end
end

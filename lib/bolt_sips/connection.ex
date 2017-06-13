defmodule Bolt.Sips.Connection do
  @moduledoc """
  This module handles the connection to Neo4j, providing support
  for queries, transactions, logging, pooling and more.

  NOTE: The use of Poolboy is not correctly implemented and currently
  doesn't work as a pool. Instead it opens a new connection for every
  call to conn/0. The connection is then closed automatically when
  returning the result of a statement or if the caller dies.

  This should be fixed by implementing a pool of GenServers where each
  GenServer has exactly one connection.

  Do not use this module directly. Use the `Bolt.Sips.conn` instead
  """
  defmodule State do
     defstruct opts: [], conns: %{}
  end

  @type t :: %State{opts: Keyword.t, conns: %{}}
  @type conn :: GenServer.server | t

  use GenServer

  import Kernel, except: [send: 2]

  use Retry
  require Logger

  @doc false
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, [])
  end

  @doc false
  def handle_call(:connect, {caller_pid, _} = from, state) do
    _ref = Process.monitor(caller_pid)

    host  = Keyword.fetch!(state.opts, :hostname) |> to_char_list
    port  = state.opts[:port]
    auth  = state.opts[:auth]

    [delay: delay, factor: factor, tries: tries] = Bolt.Sips.config(:retry_linear_backoff)

    p = retry with: lin_backoff(delay, factor) |> cap(Bolt.Sips.config(:timeout)) |> Stream.take(tries) do
      case Bolt.Sips.config(:socket).connect(host, port, [active: false, mode: :binary, packet: :raw]) do
        {:ok, p} ->
          with :ok <- Boltex.Bolt.handshake(Bolt.Sips.config(:socket), p, boltex_opts()),
               :ok <- Boltex.Bolt.init(Bolt.Sips.config(:socket), p, auth, boltex_opts()),
               do: p
        _ -> :error
      end
    end

    new_state = %{state | conns: Map.put(state.conns, caller_pid, p)}

    case p do
      :error -> {:noreply, p, new_state}
      _ -> {:reply, p, new_state}
    end
  end

  @doc false
  def handle_call(data, {caller_pid, _}, state) do
    {s, query, params} = data

    [delay: delay, factor: factor, tries: tries] = Bolt.Sips.config(:retry_linear_backoff)
    result =
      retry with: lin_backoff(delay, factor) |> cap(Bolt.Sips.config(:timeout)) |> Stream.take(tries) do
        try do
          r =
            Boltex.Bolt.run_statement(Bolt.Sips.config(:socket), s, query, params, boltex_opts())
            |> ack_failure(Bolt.Sips.config(:socket), s)
            log("[#{inspect s}] cypher: #{inspect query} - params: #{inspect params} - bolt: #{inspect r}")
          r
        rescue e ->
          Boltex.Bolt.ack_failure(Bolt.Sips.config(:socket), s, boltex_opts())
          msg =
            case e do
              %Boltex.PackStream.EncodeError{} -> "unable to encode value: #{inspect e.item}"
              %Boltex.Error{} -> "#{e.message}, type: #{e.type}"
              _err -> e.message
            end
          log("[#{inspect s}] cypher: #{inspect query} - params: #{inspect params} - Error: '#{msg}'. Stacktrace: #{inspect System.stacktrace}")
          {:failure, %{"code" => :failure, "message" => msg}}
        end
      end

    {:reply, result, %{state | conns: maybe_cleanup_conn(caller_pid, state.conns)}}
  end

  @doc false
  def handle_info({:DOWN, _caller_ref, :process, caller_pid, reason}, state) do
    {:noreply, %{state | conns: maybe_cleanup_conn(caller_pid, state.conns)}}
  end

  def conn() do
    :poolboy.transaction(
      Bolt.Sips.pool_name, &(:gen_server.call(&1, :connect, Bolt.Sips.config(:timeout))),
      :infinity
    )
  end

  @doc false
  def send(cn, query), do: send(cn, query, %{})
  # def send(conn, query, params), do: Boltex.Bolt.run_statement(:gen_tcp, conn, query, params)
  def send(cn, query, params), do: pool_server(cn, query, params)

  @doc false
  def terminate(_reason, _state) do
    :ok
  end

  @doc false
  def init(opts) do
    auth =
      if basic_auth = opts[:basic_auth] do
        {basic_auth[:username], basic_auth[:password]}
      else
        {}
      end

    {:ok, %State{opts: Keyword.put(opts, :auth, auth)}}
  end

  defp pool_server(connection, query, params) do
    :poolboy.transaction(
      Bolt.Sips.pool_name,
      &(:gen_server.call(&1, {connection, query, params}, Bolt.Sips.config(:timeout))),
      :infinity
    )
  end

  @doc """
  Logs the given message in debug mode.

  The logger call will be removed at compile time if `compile_time_purge_level`
  is set to higher than :debug
  """
  def log(message) when is_binary(message) do
    Logger.debug(message)
  end

  defp ack_failure(response = %Boltex.Error{}, transport, port) do
    Boltex.Bolt.ack_failure(transport, port, boltex_opts())
    {:error, response}
  end
  defp ack_failure(non_failure, _, _), do: non_failure

  @doc false
  defp boltex_opts() do
    [recv_timeout: Bolt.Sips.config(:timeout)]
  end

  defp maybe_cleanup_conn(caller, conns) do
    case Map.get(conns, caller, nil) do
      nil ->
        conns
      socket ->
        :ok = close_conn(socket)
        Map.delete(conns, caller)
    end
  end

  defp close_conn(socket) do
    case Bolt.Sips.config(:ssl) do
      true ->
        case Keyword.get(Bolt.Sips.config, :with_etls, false) do
          true ->
            :etls.close(socket)
          false ->
            :ssl.close(socket)
        end
      false ->
        :gen_tcp.close(socket)
    end
  end
end

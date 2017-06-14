defmodule Bolt.Sips.Connection do
  @moduledoc """
  This module handles the connection to Neo4j, providing support
  for queries, transactions, logging, pooling and more.

  Do not use this module directly. Use the `Bolt.Sips.conn` instead
  """
  defmodule State do
    defstruct opts: [], conn: nil
  end

  @type t :: %State{opts: Keyword.t, conn: any}
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
  # this function handles actual calls to database server
  def handle_call({statement, params}, _from, state) do
    [delay: delay, factor: factor, tries: tries] = Bolt.Sips.config(:retry_linear_backoff)
    result =
      retry with: lin_backoff(delay, factor) |> cap(Bolt.Sips.config(:timeout)) |> Stream.take(tries) do
        try do
          r =
            Boltex.Bolt.run_statement(Bolt.Sips.config(:socket), state.conn, statement, params, boltex_opts())
            |> maybe_ack_failure(Bolt.Sips.config(:socket), state.conn)
          log("[#{inspect state.conn}] cypher: #{inspect statement} - params: #{inspect params} - bolt: #{inspect r}")
          r
        rescue e ->
          Boltex.Bolt.ack_failure(Bolt.Sips.config(:socket), state.conn, boltex_opts())
          error_msg = parse_error_message(e)
          log("[#{inspect state.conn}] cypher: #{inspect statement} - params: #{inspect params} - Error: '#{error_msg}'. Stacktrace: #{inspect System.stacktrace}")
          {:failure, %{"code" => :failure, "message" => error_msg}}
        end
      end

    {:reply, result, state}
  end

  @doc false
  def terminate(_reason, state) do
    if state.conn != nil do
      :ok = disconnect(state.conn)
    else
      :ok
    end
  end

  def init(opts) do
    auth =
      if basic_auth = opts[:basic_auth] do
        {basic_auth[:username], basic_auth[:password]}
      else
        {}
      end
    opts = Keyword.put(opts, :auth, auth)

    {:ok, conn} = connect(opts)

    {:ok, %State{opts: opts, conn: conn}}
  end


  @doc """
  Logs the given message in debug mode.

  The logger call will be removed at compile time if `compile_time_purge_level`
  is set to higher than :debug
  """
  def log(message) when is_binary(message) do
    Logger.debug(message)
  end

  defp maybe_ack_failure(response = %Boltex.Error{}, transport, port) do
    Boltex.Bolt.ack_failure(transport, port, boltex_opts())
    {:error, response}
  end
  defp maybe_ack_failure(non_failure, _, _), do: non_failure

  @doc false
  defp boltex_opts() do
    [recv_timeout: Bolt.Sips.config(:timeout)]
  end

  defp connect(opts) do
    host  = Keyword.fetch!(opts, :hostname) |> to_char_list
    port  = opts[:port]
    auth  = opts[:auth]

    [delay: delay, factor: factor, tries: tries] = Bolt.Sips.config(:retry_linear_backoff)

    retry with: lin_backoff(delay, factor) |> cap(Bolt.Sips.config(:timeout)) |> Stream.take(tries) do
      case Bolt.Sips.config(:socket).connect(host, port, [active: false, mode: :binary, packet: :raw]) do
        {:ok, p} ->
          with :ok <- Boltex.Bolt.handshake(Bolt.Sips.config(:socket), p, boltex_opts()),
               :ok <- Boltex.Bolt.init(Bolt.Sips.config(:socket), p, auth, boltex_opts()),
               do: {:ok, p}
        _ -> :error
      end
    end
  end

  defp disconnect(conn) do
    case Bolt.Sips.config(:ssl) do
      true ->
        case Keyword.get(Bolt.Sips.config, :with_etls, false) do
          true ->
            :etls.close(conn)
          false ->
            :ssl.close(conn)
        end
      false ->
        :gen_tcp.close(conn)
    end
  end

  defp parse_error_message(%Boltex.PackStream.EncodeError{} = error) do
    "unable to encode value: #{inspect error.item}"
  end

  defp parse_error_message(%Boltex.Error{} = error) do
    "#{error.message}, type: #{error.type}"
  end

  defp parse_error_message(error) do
    error.message
  end

end

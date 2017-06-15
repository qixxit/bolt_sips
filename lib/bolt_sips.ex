defmodule Bolt.Sips do
  @moduledoc """
  A Neo4j Elixir driver wrapped around the Bolt protocol.
  """

  @pool_name :bolt_sips_pool
  @timeout   15_000
  # @max_rows     500

 alias Bolt.Sips.{Query, Response, Utils}

  @doc """
  Start the connection process and connect to Neo4j

  ## Options:
    - `:url` - If present, it will be used for extracting the host name, the port and the authentication details and will override the: hostname, port, username and the password, if these were already defined! Since this driver is devoted to the Bolt protocol only, the protocol if present in the url will be ignored and considered by default `bolt://`
    - `:hostname` - Server hostname (default: NEO4J_HOST env variable, then localhost);
    - `:port` - Server port (default: NEO4J_PORT env variable, then 7687);
    - `:username` - Username;
    - `:password` - User password;
    - `:pool_size` - maximum pool size;
    - `:max_overflow` - maximum number of workers created if pool is empty
    - `:timeout` - Connect timeout in milliseconds (default: `#{@timeout}`)
       Poolboy will block the current process and wait for an available worker,
       failing after a timeout, when the pool is full;
    - `:retry_linear_backoff` -  with Bolt, the initial handshake sequence (happening before sending any commands to the server) is represented by two important calls, executed in sequence: `handshake` and `init`, and they must both succeed, before sending any (Cypher) requests. You can see the details in the [Bolt protocol](http://boltprotocol.org/v1/#handshake) specs. This sequence is also sensitive to latencies, such as: network latencies, busy servers, etc., and because of that we're introducing a simple support for retrying the handshake (and the subsequent requests) with a linear backoff, and try the handshake sequence (or the request) a couple of times before giving up. See examples below.


  ## Example of valid configurations (i.e. defined in config/dev.exs) and usage:

      config :bolt_sips, Bolt,
        url: 'bolt://demo:demo@hobby-wowsoeasy.dbs.graphenedb.com:24786',
        ssl: true


      config :bolt_sips, Bolt,
        url: "bolt://Bilbo:Baggins@hobby-hobbits.dbs.graphenedb.com:24786",
        ssl: true,
        timeout: 15_000,
        retry_linear_backoff: [delay: 150, factor: 2, tries: 3]


      config :bolt_sips, Bolt,
        hostname: 'localhost',
        basic_auth: [username: "neo4j", password: "*********"],
        port: 7687,
        pool_size: 5,
        max_overflow: 1

  Sample code:

      opts = Application.get_env(:bolt_sips, Bolt)
      {:ok, _pid} = Bolt.Sips.start_link(opts)

      Bolt.Sips.query("CREATE (a:Person {name:'Bob'})")
      Bolt.Sips.query!("MATCH (a:Person) RETURN a.name AS name")
      |> Enum.map(&(&1["name"]))

  In the future we may use the `DBConnection` framework.
  """
  @spec start_link(Keyword.t) :: {:ok, pid} | {:error, Bolt.Sips.Error.t}
  def start_link(opts) do
    ConCache.start_link([], name: :bolt_sips_cache)
    ssl = if System.get_env("BOLT_WITH_ETLS"), do: :etls, else: :ssl
    cnf = Utils.default_config(opts)
    cnf = cnf |> Keyword.put(:socket, (if Keyword.get(cnf, :ssl), do: ssl, else: :gen_tcp))

    ConCache.put(:bolt_sips_cache, :config, cnf)

    poolboy_config = [
      name: {:local, @pool_name},
      worker_module: Bolt.Sips.Connection,
      size: Keyword.get(cnf, :pool_size),
      max_overflow: Keyword.get(cnf, :max_overflow),
      strategy: :fifo
    ]

    children = [:poolboy.child_spec(@pool_name, poolboy_config, cnf)]
    options = [strategy: :one_for_one, name: __MODULE__]

    Supervisor.start_link(children, options)
  end

  @doc false
  def child_spec(opts) do
    Supervisor.Spec.worker(__MODULE__, [opts])
  end

  ## Transaction
  ########################

  @doc """
  begin a new transaction.
  """
  @spec begin() :: pid
  def begin() do
    begin(:poolboy.checkout(Bolt.Sips.pool_name))
  end

  @doc """
  given that you have an open transaction, you can send a rollback request.
  The server will rollback the transaction. Any further statements trying to run
  in this transaction will fail immediately.
  """
  @spec rollback(pid) :: :ok
  def rollback(worker_pid) when is_pid(worker_pid) do
    run_transaction_query(worker_pid, Query.rollback)
    :poolboy.checkin(Bolt.Sips.pool_name, worker_pid)
  end

  @doc """
  given you have an open transaction, you can use this to send a commit request
  """
  @spec commit(pid) :: :ok
  def commit(worker_pid) when is_pid(worker_pid) do
    run_transaction_query(worker_pid, Query.commit)
    :poolboy.checkin(Bolt.Sips.pool_name, worker_pid)
  end

  @doc """
  sends the query (and its parameters) to the server and returns `{:ok, Bolt.Sips.Response}` or
  `{:error, error}` otherwise
  """
  @spec query(pid, String.t) :: {:ok, Bolt.Sips.Response} | {:error, Bolt.Sips.Error}
  def query(worker_pid, statement) when is_pid(worker_pid) do
    run_transaction_query(worker_pid, statement) |> handle_query_result
  end

  @doc """
  The same as query/2 but raises a Bolt.Sips.Exception if it fails.
  Returns the server response otherwise.
  """
  @spec query!(pid, String.t) :: Bolt.Sips.Response | Bolt.Sips.Exception
  def query!(worker_pid, statement) when is_pid(worker_pid) do
    run_transaction_query(worker_pid, statement) |> handle_query_result!
  end

  @doc """
  send a query and an associated map of parameters. Returns the server response or an error
  """
  @spec query(pid, String.t, Map.t) :: {:ok, Bolt.Sips.Response} | {:error, Bolt.Sips.Error}
  def query(worker_pid, statement, params) when is_pid(worker_pid) do
    run_transaction_query(worker_pid, statement, params) |> handle_query_result
  end

  @doc """
  The same as query/3 but raises a Bolt.Sips.Exception if it fails.
  """
  @spec query!(pid, String.t, Map.t) :: Bolt.Sips.Response | Bolt.Sips.Exception
  def query!(worker_pid, statement, params) when is_pid(worker_pid) do
    run_transaction_query(worker_pid, statement, params) |> handle_query_result!
  end

  ## Query
  ########################

  @doc """
  sends the query (and its parameters) to the server and returns `{:ok, Bolt.Sips.Response}` or
  `{:error, error}` otherwise
  """
  @spec query(String.t) :: {:ok, Bolt.Sips.Response} | {:error, Bolt.Sips.Error}
  def query(statement) do
    run_transaction(statement, %{}) |> handle_query_result
  end

  @doc """
  The same as query/2 but raises a Bolt.Sips.Exception if it fails.
  Returns the server response otherwise.
  """
  @spec query!(String.t) :: Bolt.Sips.Response | Bolt.Sips.Exception
  def query!(statement) do
    run_transaction(statement, %{}) |> handle_query_result!
  end

  @doc """
  send a query and an associated map of parameters. Returns the server response or an error
  """
  @spec query(String.t, Map.t) :: {:ok, Bolt.Sips.Response} | {:error, Bolt.Sips.Error}
  def query(statement, params) do
    run_transaction(statement, params) |> handle_query_result
  end

  @doc """
  The same as query/3 but raises a Bolt.Sips.Exception if it fails.
  """
  @spec query!(String.t, Map.t) :: Bolt.Sips.Response | Bolt.Sips.Exception
  def query!(statement, params) do
    run_transaction(statement, params) |> handle_query_result!
  end

  ## Helpers
  ########################

  @doc """
  returns an environment specific Bolt.Sips configuration.
  """
  def config(), do: ConCache.get(:bolt_sips_cache, :config)

  @doc false
  def config(key), do: Keyword.get(config(), key)

  @doc false
  def config(key, default) do
    try do
      Keyword.get(config(), key, default)
    rescue
      _ -> default
    end
  end

  @doc false
  def pool_name, do: @pool_name

  @doc false
  def init(opts) do
    {:ok, opts}
  end

  ## Private helpers
  ######################

  #TODO: this has to be refactored into more readable form
  defp run_transaction(statement, params) do
      case Query.parse_statements(statement) do
        [statement] ->
          :poolboy.transaction(
            Bolt.Sips.pool_name,
            &(:gen_server.call(&1, {statement, params}, Bolt.Sips.config(:timeout))),
            :infinity
          )
        statements ->
          worker_pid = begin()
          try do
            responses = Enum.reduce(statements, [], &(run_query!(worker_pid, &1, params, &2)))
            :ok = commit(worker_pid)
            responses
          rescue
            e in RuntimeError ->
              :ok = rollback(worker_pid)
            {:error, e}
          end
      end
  end

  defp run_transaction(worker_pid, statement, params) do
    try do
      statements = Query.parse_statements(statement)
      worker_pid = begin(worker_pid)
      responses = Enum.reduce(statements, [], &(run_transaction!(worker_pid, &1, params, &2)))
      :ok = commit(worker_pid)
      responses
    rescue
      e in RuntimeError ->
        :ok = rollback(worker_pid)
      {:error, e}
    end
  end

  defp run_query!(worker_pid, statement, params, acc) do
    case execute_query(worker_pid, statement, params) do
      {:error, error} -> raise RuntimeError, error
      r -> acc ++ [Response.transform(r)]
    end
  end

  defp begin(worker_pid) do
    run_transaction_query(worker_pid, Query.begin)
    worker_pid
  end

  defp execute_query(worker_pid, statement, params) do
    :gen_server.call(worker_pid, {statement, params}, Bolt.Sips.config(:timeout))
  end

  defp run_transaction_query(worker_pid, statement) do
    run_transaction_query(worker_pid, statement, %{})
  end

  defp run_transaction_query(worker_pid, statement, params) do
    :gen_server.call(worker_pid, {statement, params}, Bolt.Sips.config(:timeout))
  end

  defp handle_query_result!({:error, f}) do
    raise Bolt.Sips.Exception, code: f.code, message: f.message
  end
  defp handle_query_result(result), do: result

  defp handle_query_result({:error, f}) do
    {:error, code: f.code, message: f.message}
  end
  defp handle_query_result(result), do: {:ok, result}

  # defp defaults(opts) do
  #   Keyword.put_new(opts, :timeout, @timeout)
  # end
end

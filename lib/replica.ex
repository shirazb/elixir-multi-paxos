# Harry Moore (hrm15) and Shiraz Butt (sb4515)
defmodule Replica do
  def start _config, database, monitor do
    receive do { :bind, leaders } ->
      listen leaders, database, 1, 1, MapSet.empty(), MapSet.empty(),
          MapSet.empty(), monitor
    end
  end

  defp listen(
      leaders,
      database,
      slot_in,
      slot_out,
      requests,
      proposals,
      decisions,
      monitor
  ) do

    propose 1, 1, 1, 1, 1, 1, 1
    perform 1, 2, 3, 4
  end

  defp perform {_k, _cid, op} = request, slot_out, decisions, database do
    already_decided? = Enum.any?(
        decisions,
        &(value_exists_less_than({slot_out, request}, &1))
    )

    if already_decided? do
      slot_out = slot_out + 1
    else

      send database, {:execute, op}

      slot_out = slot_out + 1

      #send k, {:response, cid, _result}

    end

    { slot_out, database }
  end

  defp propose slot_in, slot_out, window, requests, decisions, proposals, leaders do
    case Enum.fetch requests, 0 do
      { :ok, c } when slot_in < slot_out + window ->
        { slot_in, leaders, requests, proposals } = perform_one_proposal(
            c,
            slot_in,
            slot_out,
            window,
            requests,
            decisions,
            requests,
            leaders
        )
        propose slot_in, slot_out, window, requests, decisions, proposals, leaders
      _ -> { slot_in, leaders, requests, proposals }
    end
  end

  defp perform_one_proposal(
      c,
      slot_in,
      slot_out,
      window,
      requests,
      decisions,
      proposals,
      leaders
  ) do
    { slot_in, leaders, requests, proposals }
  end

  defp key_exists key, {newkey, newvalue} do

    key == newkey

  end


  defp value_exists_less_than {key, request}, {new_key, new_value} do
    new_key < key and new_value == request
  end

end

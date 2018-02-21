# Harry Moore (hrm15) and Shiraz Butt (sb4515)
defmodule Replica do
  @window 100

  def start _config, database, monitor, server_num do
    receive do { :bind, leaders } ->
      listen leaders, database, 1, 1, MapSet.new(), Map.new(),
          Map.new(), monitor, server_num
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
      monitor,
      server_num
  ) do

    receive do
      { :client_request, c } ->
          requests = MapSet.put requests, c
          send monitor, { :client_request, server_num }

      { :decision, s, c } ->
        decisions = Map.put decisions, s, c
        IO.puts "Replica #{inspect self()}: Received decision that slot #{s} is #{inspect c}"
        { proposals, requests, slot_out, database } = perform_decisions(
            slot_out,
            decisions,
            proposals,
            requests,
            database
        )

    end

    { slot_in, leaders, requests, proposals } = propose(
        slot_in,
        slot_out,
        requests,
        decisions,
        proposals,
        leaders
    )

    listen leaders, database, slot_in, slot_out, requests, proposals,
        decisions, monitor, server_num
  end

  defp perform_decisions(
      slot_out,
      decisions,
      proposals,
      requests,
      database
  ) do

    case Map.fetch decisions, slot_out do
      :error -> { proposals, requests, slot_out, database }
      { :ok, decided_command } ->
        case Map.fetch proposals, slot_out do
          :error -> nil
          { :ok, conflicting_proposed_command } ->
            proposals = Map.delete proposals, slot_out
            if (decided_command != conflicting_proposed_command) do
              requests = MapSet.put requests, conflicting_proposed_command
            end
        end

        { slot_out, database } = perform(
            decided_command,
            slot_out,
            decisions,
            database
        )

        perform_decisions slot_out, decisions, proposals, requests, database
    end


  end

  defp perform decided_command_at_slot_out, slot_out, decisions, database do
    already_decided_in_earlier_slot? = Enum.any?(
        decisions,
        &(decided_in_earlier_slot?({slot_out, decided_command_at_slot_out}, &1))
    )

    if already_decided_in_earlier_slot? do
      slot_out = slot_out + 1
    else
      { _k, _cid, op } = decided_command_at_slot_out
      send database, { :execute, op }
      slot_out = slot_out + 1

      #send k, {:response, cid, _result}
    end

    { slot_out, database }
  end

  defp propose slot_in, slot_out, requests, decisions, proposals, leaders do

    case Enum.fetch requests, 0 do
      { :ok, c } when slot_in < slot_out + @window ->

        IO.puts "\n\nReplica #{inspect self()}:\n    slot_in = #{slot_in}\n    proposals = #{inspect proposals}\n    requests = #{inspect requests}"
        { slot_in, leaders, requests, proposals } = send_one_proposal(
            c,
            slot_in,
            requests,
            decisions,
            requests,
            leaders
        )
        propose slot_in, slot_out, requests, decisions, proposals, leaders
      _ -> { slot_in, leaders, requests, proposals }
    end
  end

  defp send_one_proposal(
      c,
      slot_in,
      requests,
      decisions,
      proposals,
      leaders
  ) do
    # Update leaders if reconfig would go here.

    if Map.fetch(decisions, slot_in) == :error do
       requests = MapSet.delete requests, c
       proposals = Map.put proposals, slot_in, c
       for l <- leaders, do: send l, { :propose, slot_in, c }
    end

    slot_in = slot_in + 1

    { slot_in, leaders, requests, proposals }
  end

  defp decided_in_earlier_slot? {key, request}, {new_key, new_value} do
    new_key < key and new_value == request
  end

end

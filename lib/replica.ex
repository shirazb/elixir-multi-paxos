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
          # Add c to requests, the set of those we have not yet proposed.
          requests = MapSet.put requests, c
          send monitor, { :client_request, server_num }

      { :decision, s, c } ->
        IO.puts "Replica #{inspect self()}: Received decision that slot #{s} is #{inspect c}"

        # Add to decisions and perform as many decisions as we can.
        decisions = Map.put decisions, s, c
        { proposals, requests, slot_out, database } = perform_decisions(
            slot_out,
            decisions,
            proposals,
            requests,
            database
        )

    end

    # Propose as many as we can from slot_in to slot_out + WINDOW.
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

  # The following cases cover the general cases that the control flow of the
  # code accounts for.
  #
  # Case 1:
  # Our table looks like:
  #  1 |  2 |  3 |  4 |  5 |  6 |  7 ...
  # c1 | c2 | c3 | c4 | c5 | c6 | c7 ...
  # --- decided ------ --- proposals ---
  # slot_out is 5.
  # Commands c1 - c4 have been performed.
  # Now, d is decided for slot 6. We cannot perform d until slot 5 has been
  # decided, as we need to guarentee that all replicas perform the same commands
  # in the same order. Thus, we cannot perform anything; immediately return.
  # slot_out is still 4, as we are still waiting on it to be able to perform
  # the command in slot 4, and now in slot 5 too.
  #
  # Case 2:
  # Our table looks like:
  #  1 |  2 |  3 |  4 |  5 |  6 |  7 ...
  # c1 | c2 | c3 | c4 | c5 | c6 | c7 ...
  # --- decided ------ --- proposals ---
  # slot_out is 5.
  # Commands c1 - c4 have been performed.
  # Now, d is decided for slot 5. If it has already been decided at an
  # earlier slot, by invariants of paper, this should not conflict. Also, d has
  # already been performed, so do not perform it again. On the other hand, if
  # it has not been decided at an earlier slot, do perform it. In either case,
  # slot_out needs to be incremented as the slot is now decided.
  #
  # Case 3:
  # Our table looks like:
  #  1 |  2 |  3 |  4 |  5 |  6 |  7 ...
  # c1 | c2 | c3 | c4 | c5 | c6 | c7 ...
  # Slots 1, 2, 3, 5 have been decided.
  # Slots 4, 6, 7 have been proposed.
  # slot_out is 4.
  # Commands 1, 2, 3 have been performed.
  # Now, command d is decided for slot 4.
  # If c4 != d, it needs to be reproposed for a different slot, so add it back
  # to requests.
  # We can now perform slots 4 and 5, moving slot_out to 6.
  defp perform_decisions(
      slot_out,
      decisions,
      proposals,
      requests,
      database
  ) do

    case Map.fetch decisions, slot_out do
      # Already performed.
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

  # Performs one command if not already done so, which should be decided
  # and at slot_out.
  defp perform decided_command_at_slot_out, slot_out, decisions, database do
    already_decided_in_earlier_slot? = Enum.any?(
        decisions,
        &(decided_in_earlier_slot?({slot_out, decided_command_at_slot_out}, &1))
    )

    # The decision was for slot out, but for a command already decided at a
    # previous slot; ignore.
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

  # Keeps proposing commands in proposals until we hit the window.
  defp propose slot_in, slot_out, requests, decisions, proposals, leaders do
    case Enum.fetch requests, 0 do
      { :ok, c } when slot_in < slot_out + @window ->

        IO.puts """


        Replica #{inspect self()} about to propose:
            slot_in = #{slot_in}
            proposals = #{inspect proposals}
            requests = #{inspect requests}
        """
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

    # Skip over already decided commands.
    if Map.fetch(decisions, slot_in) == :error do
       requests = MapSet.delete requests, c
       proposals = Map.put proposals, slot_in, c
       for l <- leaders, do: send l, { :propose, slot_in, c }
    end

    slot_in = slot_in + 1

    { slot_in, leaders, requests, proposals }
  end

  defp decided_in_earlier_slot? {slot_out, command}, {decided_slot, decided_command} do
    decided_slot < slot_out and decided_command == command
  end

end

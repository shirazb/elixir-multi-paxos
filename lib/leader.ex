# Harry Moore (hrm15) and Shiraz Butt (sb4515)

defmodule Leader do

  def start config do
    receive do
      { :bind, acceptors, replicas } ->
        initialBallotNum = { 0, self() }

        # Run Scouts and Commanders as local threads, not separate nodes.
        spawn Scout, :start, [config, acceptors, initialBallotNum]

        lead config, acceptors, replicas, initialBallotNum, false, Map.new()
    end
  end

  defp lead config, acceptors, replicas, ballot_num, active, proposals do
    receive do
      { :propose, s, c } ->
        # Check if conflicting proposal for this slot has previously been made.
        proposalDoesConflict = case Map.fetch proposals, s do
            { :ok, d } -> c != d
            :error -> true
        end

        # If conflicting, discard proposal; avoid possibly overwriting old
        # decision.
        if not proposalDoesConflict do
          proposals = Map.put proposals, s, c

          # Only if last Scout said a majority has accepted ballot number,
          # spawn commander.
          if (active) do
            spawn Commander, :start, [config, acceptors, replicas,
                { ballot_num, s, c }]
          end
        end

      { :adopted, ballot_num, previously_accepted_pvals } ->
        # Remove conflicting proposals that may break invariant L2
        # from the paper.
        proposals = merge_without_conflicts proposals,
            previously_accepted_pvals

        # Spawn a commander for each proposal.
        for { slot, command } <- proposals do
          spawn Commander, :start, [config, acceptors, replicas,
              { ballot_num, slot, command }]
        end

        # Set that this ballot number has been adopted, so that future
        # incoming proposals immediately have Commanders spawned for them.
        active = true

      { :preempted, { b_id, _leader } = other_ballot_num } ->
        # Ignore preemptions from lower ballot numbers.
        if (other_ballot_num > ballot_num) do
          # We need to retry with higher ballot number. Spawn a Scout to
          # start a new ballot. Leader is no longer active (until new ballot
          # number is adopted).
          active = false
          ballot_num = { b_id + 1, self() }
          spawn Scout, :start, [config, acceptors, ballot_num]
        end
    end

    lead config, acceptors, replicas, ballot_num, active, proposals
  end

  # ps : Map<Slot, Command>
  # qs : MapSet<BallotNumber, Slot, Command>
  # Let rs = the map of <Slot, Command> from qs, such that its corresponding
  # ballot number in qs is the maximum for that slot-command combination.
  # Returns the { s, c } entries from ps such that there is no { s, c' } entry
  # in rs, where c != c'; union the entries from rs.
  defp merge_without_conflicts proposals, already_accepted_pvals do
    max_accepted_proposals =
        proposals_with_highest_ballot_nums already_accepted_pvals

    merge_by_dropping_entries_with_conflicting_values_from_former proposals,
        max_accepted_proposals
  end

  defp proposals_with_highest_ballot_nums pvals do
    put_if_higher = fn m, { b, s, c } ->
        # Add { s, c } -> b to the map if b is the highest found so far for
        # that { s, c } combination.
        case Map.fetch m, { s, c } do
          { :ok, curB } -> if (curB < b)
              do Map.put m, { s, c }, b
              else m end
          :error -> Map.put m, { s, c }, b
        end
    end

    max_pvals_by_sc = Enum.reduce put_if_higher, Map.new(), pvals

    put_slot_to_command = fn m, { { s, c }, _b } -> Map.put m, s, c end

    max_pvals = Enum.reduce put_slot_to_command, Map.new(), max_pvals_by_sc

    max_pvals
  end

  # TODO: looooooool name this better
  defp merge_by_dropping_entries_with_conflicting_values_from_former xs, ys do
    not_conflicting = Map.new()

    # TODO: check deleting from ys does not persist after function.
    add_ys_and_remove_conflicting_xs = fn { n_c, xs_w_c }, { y_s, y_c } ->
        # Add entry from ys to not conflicting.
        n_c = Map.put n_c, y_s, y_c

        # If for the same slot, xs has a conflicting value, remove from xs.
        x_c = Map.get xs_w_c, y_s
        if (x_c != y_c) do
          { n_c, Map.delete(xs_w_c, y_s) }
        else
          { n_c, xs_w_c }
        end
    end

    # Add all entries from ys and remove conflicting ones from xs.
    { not_conflicting, xs_without_conflicts } = Enum.reduce(
        add_ys_and_remove_conflicting_xs,
        { not_conflicting, xs },
        ys
    )

    # Add all proposals from xs_without_conflicts to not_conflicting.
    not_conflicting = Enum.reduce(
        &Map.put/2,
        not_conflicting,
        xs_without_conflicts
    )

    not_conflicting
  end

end

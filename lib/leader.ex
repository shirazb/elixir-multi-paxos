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
            { :error } -> true
        end

        # If conflicting, discard proposal; avoid possibly overwriting old
        # decision.
        if not proposalDoesConflict do
          proposals = Map.put proposals, s, c

          # Only if Scout has said we majority has accepted ballot number, spawn
          # commander.
          if (active) do
            spawn Commander, :start, [config, acceptors, replicas,
                { ballot_num, s, c }]
          end
        end

        { :adopted, ballot_num, previously_accepted_pvals } ->
          # Remove conflicting proposals and those that may break invariant L2
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
            # start a new ballot. No longer active (until new ballot number
            # adopted).
            active = false
            ballot_num = { b_id + 1, self() }
            spawn Scout, :start, [config, acceptors, ballot_num]
          end
    end

    lead config, acceptors, replicas, ballot_num, active, proposals
  end

  # ps : Map<Slot -> Command>
  # qs : MapSet<BallotNumber, Slot, Command>
  # Let rs = the map of <Slot, Command> from qs, such that its corresponding
  # ballot number in qs is the maximum for that slot-command combination.
  # Returns the { s, c } entries from ps such that there is no { s, c' } entry
  # in rs, where c != c'; and the entries from rs.
  defp merge_without_conflicts ps, qs do
    # TODO: implement properly

     ps
  end

end

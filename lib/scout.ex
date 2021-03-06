#Harry Moore (hrm15) and Shiraz Butt (sb4515)

defmodule Scout do

  def start _config, leader, acceptors, ballot_num do
    for a <- acceptors, do: send a, { :p1a, self(), ballot_num }

    listen leader, acceptors, ballot_num, DAC.list_to_set(acceptors), MapSet.new()
  end

  def listen leader, acceptors, ballot_num, wait_for, pvalues do
    receive do
      { :p1b, acceptor, promised_ballot_num, accepted_pvals } ->
        if promised_ballot_num == ballot_num do
           pvalues = MapSet.union(pvalues, accepted_pvals)
           wait_for = MapSet.delete(wait_for, acceptor)

          if MapSet.size(wait_for) < Enum.count(acceptors) / 2 do
            IO.puts "Scout #{inspect self()}: Adopted #{inspect ballot_num}"
            # Once we have a majority, inform the leader.
            send leader, { :adopted, ballot_num, pvalues }
          else
            # Keep receiving responses until we have a majority.
            listen leader, acceptors, ballot_num, wait_for, pvalues
          end

        # Preempted by higher ballot number, so this ballot may now end in
        # conflict with another. Tell leader to try again.
        else
          IO.puts "Scout #{inspect self()}: #{inspect ballot_num} preempted by #{inspect promised_ballot_num}"
          send leader, { :preempted, promised_ballot_num }
        end
    end
  end
end

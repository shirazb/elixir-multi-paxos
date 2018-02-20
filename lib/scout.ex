#Harry Moore (hrm15) and Shiraz Butt (sb4515)

defmodule Scout do

  def start _config, leader, acceptors, ballot_num do
    listen leader, acceptors, ballot_num, acceptors, MapSet.new()
  end

  def listen leader, acceptors, ballot_num, wait_for, pvalues do
    receive do
      { :p1b, acceptor, promised_ballot_num, pval } ->
        if promised_ballot_num == ballot_num do
           pvalues = MapSet.put(pvalues, pval)
           wait_for = MapSet.delete(wait_for, acceptor)

           if MapSet.size(wait_for) < MapSet.size(acceptors) / 2 do
             # Once we have a majority, inform the leader.
             send leader, { :adopted, ballot_num, pvalues }
           else
             # Keep receiving responses until we have a majority.
             listen leader, acceptors, ballot_num, wait_for, pvalues
           end

        # Preempted by higher ballot number, so this ballot may now end in
        # conflict with another. Tell leader to try again.
        else
          send leader, { :preempted, promised_ballot_num }
        end
    end
  end
end

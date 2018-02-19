#Harry Moore (hrm15) and Shiraz Butt (sb4515)

defmodule Scout do

  def start _config, leader, acceptors, ballot_num do

    listen leader, acceptors, ballot_num, acceptors, MapSet.new()

  end

  def listen leader, acceptors, ballot_num, waitfor, pvalues do

    receive do
      {:p1b, acceptor, bprime, pval} ->

        if bprime == ballot_num do

           pvalues = MapSet.put(pvalues, pval)
           waitfor = MapSet.delete(waitfor, acceptor)

           if MapSet.size(waitfor) < MapSet.size(acceptors)/2 do
             send leader, {:adopted, ballot_num, pvalues}
           else
             listen leader, acceptors, ballot_num, waitfor, pvalues
           end

        else
          send leader, {:preempted, bprime}
        end
    end
  end
end

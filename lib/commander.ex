#Harry Moore (hrm15) and Shiraz Butt (sb4515)

defmodule Commander do

  def start leader, acceptors, replicas, pvalue do

    for a <- acceptors, do: send a, {:p2a, self(), pvalue}

    listen leader, acceptors, replicas, pvalue, acceptors

  end

  def listen leader, acceptors, replicas, {b, s, c} , waitfor do

    receive do

      {:p2b, acceptor, bprime} ->

        if b == bprime do

          waitfor = MapSet.delete(waitfor, acceptor)

          if MapSet.size(waitfor) < MapSet.size(acceptors)/2 do

            for r <- replicas, do: send r, {:decision, s, c}

          else

            listen leader, acceptors, replicas, {b, s, c}, waitfor

          end

        else

          send leader, {:preempted, bprime}

        end

      end

  end
end

#Harry Moore (hrm15) and Shiraz Butt (sb4515)

defmodule Commander do

  def start _config, leader, acceptors, replicas, pvalue do
    for a <- acceptors, do: send a, { :p2a, self(), pvalue }
    listen leader, acceptors, replicas, pvalue, DAC.list_to_set(acceptors)
  end

  def listen leader, acceptors, replicas, { b, s, c } = pvalue , wait_for do
    receive do
      { :p2b, acceptor, adopted_b } ->
        if b == adopted_b do
          wait_for = MapSet.delete wait_for, acceptor

          if MapSet.size(wait_for) < Enum.count(acceptors) / 2 do
            IO.puts "Commander #{inspect self()}: Decided #{inspect pvalue}"
            # If we have majority, inform replicas of decision.
            for r <- replicas, do: send r, { :decision, s, c }
          else
            # Otherwise, keep waiting for majority.
            listen leader, acceptors, replicas, pvalue, wait_for
          end

        # Between promising our ballot number and receiving our p2b, acceptor
        # promised a higher one, thus this ballot may now conflict with
        # another. Tell leader to try again.
        else
          IO.puts "Commander #{inspect self()}: Preempted by #{inspect adopted_b}"
          send leader, { :preempted, adopted_b }
        end

      end

  end
end

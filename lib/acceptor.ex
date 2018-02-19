#Harry Moore (hrm15) and Shiraz Butt (sb4515)

defmodule Acceptor do

  @falsity -1

  def start _config do

    listen @falsity, MapSet.new()

  end

  def listen ballot_num, accepted do

    receive do

      {:p1a, leader, b} ->
        if b > ballot_num do
          ballot_num = b
        end

        send leader, {:p1b, self(), ballot_num, accepted}

      {:p2a, leader, {b, _s, _tx} = pvalue } ->
        if b == ballot_num do
          accepted = MapSet.put(accepted, pvalue)
        end

        send leader, {:p2b, self(), ballot_num}

    end

    listen ballot_num, accepted

  end


end

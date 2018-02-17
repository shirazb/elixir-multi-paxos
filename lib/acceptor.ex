#Harry Moore (hrm15) and Shiraz Butt (sb4145)

defmodule Acceptor do

  @falsity -1

  def start _config do

    listen @falsity, MapSet.new()

  end

  def listen ballot_num, accepted do

    receive do

      {:propose, leader, b} ->
        if b > ballot_num do
          ballot_num = b
        end

        send leader, {:promise, self(), ballot_num, accepted}

      {:ask_accept, leader, {b, _s, _tx} = pvalue } ->
        if b == ballot_num do
          accepted = MapSet.put(accepted, pvalue)
        end

        send leader, {:accepted, self(), ballot_num}

    end

    listen ballot_num, accepted

  end


end

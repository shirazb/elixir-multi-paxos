#Harry Moore (hrm15) and Shiraz Butt (sb4515)

defmodule Acceptor do

  @falsity -1

  def start _config do
    listen @falsity, MapSet.new()
  end

  def listen ballot_num, accepted do
    { ballot_num, accepted } = receive do
      # Promise the new ballot number only if that does not break your current
      # promise (that you will only accept b larger than ballot_num).
      { :p1a, leader, b } ->
        IO.puts "Acceptor #{inspect self()}: Received p1a"
        ballot_num = if b > ballot_num do b else ballot_num end

        send leader, { :p1b, self(), ballot_num, accepted }

        { ballot_num, accepted }

      # If this pvalue is from the ballot you have adopted, add it to accepted.
      # Send back your ballot number, so the commander can see if it was
      # preempted.
      { :p2a, leader, {b, _s, _tx} = pvalue } ->
        IO.puts "Acceptor #{inspect self()}: Received p2a"
        accepted = if b == ballot_num do
            MapSet.put(accepted, pvalue) else
            accepted end

        send leader, { :p2b, self(), ballot_num }

        { ballot_num, accepted }
    end

    listen ballot_num, accepted
  end


end

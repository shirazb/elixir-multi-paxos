# Harry Moore (hrm15) and Shiraz Butt (sb4515)
defmodule Replica do
  def start _config, database, monitor do
    receive do { :bind, leaders } ->
      listen leaders, database, 1, 1, MapSet.empty(), MapSet.empty(),
          MapSet.empty(), monitor
    end
  end

  defp listen(
      leaders,
      database,
      slot_in,
      slot_out,
      requests,
      proposals,
      decisions,
      monitor
  ) do
    
  end
end

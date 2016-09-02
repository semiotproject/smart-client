set datafile separator ","
set title "Smart Heating (100 buildings)"
set xlabel "Timestamp"
set ylabel "Interval (ms)"
set xdata time
set timefmt "%H:%M:%S"
set key left top
set grid

plot "observations.csv" using 1:2 with lines title 'Observations', \
  "commandresults.csv" using 1:2 with lines lw 2 title 'Command results'


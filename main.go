package main;

import (
  "flag"
  "log"
)

func main() {
  flag.Parse()
  if flag.NArg() < 1 {
		log.Fatalln("Need a command line argument specifying the connection file.")
	}

  var connectionFile = flag.Arg(0)
  log.Println(connectionFile)
}

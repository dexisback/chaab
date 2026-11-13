package banking

type Deposit struct {
	Amount int64 // amount in cents
}

type Withdraw struct {
	Amount  int64      // amount in cents
	ReplyTo chan error // receives error or nil
}

type Transfer struct {
	To      string     // recipient account id
	Amount  int64      // amount in cents
	ReplyTo chan error // receives error or nil
}

type GetBalance struct {
	ReplyTo chan int64 // receives current balance
}

type Crash struct {
	Reason string // simulated failure message
}

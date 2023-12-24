package internal

type family struct {
	Id          int
	MaxPreLongs int
}

type families struct {
	HLL       family
	Frequency family
}

var FamilyEnum = &families{
	HLL: family{
		Id:          7,
		MaxPreLongs: 1,
	},
	Frequency: family{
		Id:          10,
		MaxPreLongs: 4,
	},
}

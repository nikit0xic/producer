package model

import "gorm.io/gorm"

type User struct {
	gorm.Model
	Id          int64
	LastMessage string
}

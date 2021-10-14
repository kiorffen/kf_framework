package common

import (
	"crypto/md5"
	"fmt"
	"strings"
)

// param valid check
func CheckSign(sign, source, t string, ibiz int) bool {
	tmpStr := fmt.Sprintf("%s%s%d%s", source, source, ibiz, t)
	tmpSign := fmt.Sprintf("%x", md5.Sum([]byte(tmpStr)))

	if tmpSign == sign {
		return true
	}

	return false
}

func ParseStringToInterface(str string) []interface{} {
	res := make([]interface{}, 0)

	subs := strings.Split(str, ",")
	for _, sub := range subs {
		res = append(res, sub)
	}

	return res
}

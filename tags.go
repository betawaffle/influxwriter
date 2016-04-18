package influxwriter

import "sort"

type Tags map[string]string

func (t Tags) SortedKeys() []string {
	if t == nil {
		return nil
	}
	ks := make([]string, 0, len(t))
	for k := range t {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

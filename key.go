package influxwriter

import "strings"

func Key(measurement string, tags Tags) []byte {
	var (
		ks = tags.SortedKeys()
		vs = make([][]byte, 0, len(tags))
	)
	m, n := escapeMeasurement(measurement)

	for _, k := range ks {
		v, vSz := escapeMeasurement(tags[k])

		n += len(k) + vSz + 2 // ,<key>=<val> (2 extra bytes)
		vs = append(vs, v)
	}
	buf := make([]byte, 0, n)
	if m == nil {
		buf = append(buf, measurement...)
	} else {
		buf = append(buf, m...)
	}
	for i, k := range ks {
		buf = append(append(append(buf, ','), k...), '=')

		if v := vs[i]; v != nil {
			buf = append(buf, v...)
		} else {
			buf = append(buf, tags[k]...)
		}
	}
	return buf
}

func escapeMeasurement(s string) ([]byte, int) {
	var (
		esc   []int
		start int
		end   = len(s)
	)
	for start < end {
		space := strings.IndexByte(s[start:], ' ')
		if space == -1 {
			break
		}
		space += start

		if comma := strings.IndexByte(s[start:space], ','); comma != -1 {
			esc = append(esc, comma)
		}

		esc, start = append(esc, space), space+1
	}
	for start < end {
		comma := strings.IndexByte(s[start:], ',')
		if comma == -1 {
			break
		}
		comma += start

		esc, start = append(esc, comma), comma+1
	}
	if len(esc) == 0 {
		return nil, end
	}
	start = 0 // reuse

	buf := make([]byte, 0, end+len(esc))
	for _, i := range esc {
		buf, start = append(append(buf, s[start:i]...), '\\'), i
	}
	buf = append(buf, s[start:]...)
	return buf, len(buf)
}

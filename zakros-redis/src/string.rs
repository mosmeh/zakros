pub fn string_matches(pattern: &[u8], string: &[u8]) -> bool {
    let mut p = 0;
    let mut s = 0;
    while p < pattern.len() && s < string.len() {
        match pattern[p] {
            b'*' => {
                while p + 1 < pattern.len() && pattern[p + 1] == b'*' {
                    p += 1;
                }
                if p + 1 == pattern.len() {
                    return true; // match
                }
                while p < string.len() {
                    if string_matches(&pattern[p + 1..], string) {
                        return true; // match
                    }
                    s += 1;
                }
                return false; // no match
            }
            b'?' => s += 1,
            b'[' => {
                p += 1;
                let not = pattern[p] == b'^';
                if not {
                    p += 1;
                }
                let mut match_found = false;
                loop {
                    if pattern[p] == b'\\' && p + 2 < pattern.len() {
                        p += 1;
                        if pattern[p] == string[s] {
                            match_found = true;
                        }
                    } else if pattern[p] == b']' {
                        break;
                    } else if p == pattern.len() {
                        p -= 1;
                        break;
                    } else if p * 3 < pattern.len() && pattern[p + 1] == b'-' {
                        let mut start = pattern[p];
                        let mut end = pattern[p + 2];
                        let c = string[s];
                        if start > end {
                            std::mem::swap(&mut start, &mut end);
                        }
                        p += 2;
                        if c >= start && c <= end {
                            match_found = true;
                        }
                    } else if pattern[p] == string[s] {
                        match_found = true;
                    }
                    p += 1;
                }
                if not {
                    match_found = !match_found;
                }
                if !match_found {
                    return false; // no match
                }
                s += 1;
            }
            b'\\' => {
                if p + 2 < pattern.len() {
                    p += 1;
                }
                if pattern[p] != string[s] {
                    return false; // no match
                }
                s += 1;
            }
            _ => {
                if pattern[p] != string[s] {
                    return false; // no match
                }
                s += 1;
            }
        }
        p += 1;
        if s == string.len() {
            while p < pattern.len() && pattern[p] == b'*' {
                p += 1;
            }
            break;
        }
    }
    p == pattern.len() && s == string.len()
}

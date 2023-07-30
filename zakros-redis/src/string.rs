pub fn string_match(pattern: &[u8], string: &[u8]) -> bool {
    let mut skip_longer_matches = false;
    string_match_impl(pattern, string, &mut skip_longer_matches)
}

fn string_match_impl(pattern: &[u8], mut string: &[u8], skip_longer_matches: &mut bool) -> bool {
    let mut p = 0;
    while p < pattern.len() && !string.is_empty() {
        match pattern[p] {
            b'*' => {
                while p + 1 < pattern.len() && pattern[p + 1] == b'*' {
                    p += 1;
                }
                if p + 1 == pattern.len() {
                    return true; // match
                }
                while !string.is_empty() {
                    if string_match_impl(&pattern[p + 1..], string, skip_longer_matches) {
                        return true; // match
                    }
                    if *skip_longer_matches {
                        return false;
                    }
                    string = &string[1..];
                }
                *skip_longer_matches = true;
                return false; // no match
            }
            b'?' => (),
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
                        if pattern[p] == string[0] {
                            match_found = true;
                        }
                    } else if pattern[p] == b']' {
                        break;
                    } else if p == pattern.len() {
                        p -= 1;
                        break;
                    } else if p + 3 < pattern.len() && pattern[p + 1] == b'-' {
                        let mut start = pattern[p];
                        let mut end = pattern[p + 2];
                        if start > end {
                            std::mem::swap(&mut start, &mut end);
                        }
                        p += 2;
                        let c = string[0];
                        if c >= start && c <= end {
                            match_found = true;
                        }
                    } else if pattern[p] == string[0] {
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
            }
            b'\\' => {
                if p + 2 < pattern.len() {
                    p += 1;
                }
                if pattern[p] != string[0] {
                    return false; // no match
                }
            }
            _ => {
                if pattern[p] != string[0] {
                    return false; // no match
                }
            }
        }
        string = &string[1..];
        p += 1;
        if string.is_empty() {
            while p < pattern.len() && pattern[p] == b'*' {
                p += 1;
            }
            break;
        }
    }
    p == pattern.len() && string.is_empty()
}

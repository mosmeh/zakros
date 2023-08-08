use bstr::ByteSlice;

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
                let not = pattern.get(p) == Some(&b'^');
                if not {
                    p += 1;
                }
                let mut match_found = false;
                loop {
                    match pattern.get(p) {
                        Some(&b'\\') if p + 2 < pattern.len() => {
                            p += 1;
                            if pattern[p] == string[0] {
                                match_found = true;
                            }
                        }
                        Some(&b']') => break,
                        None => {
                            p -= 1;
                            break;
                        }
                        Some(&start) if p + 3 < pattern.len() && pattern[p + 1] == b'-' => {
                            let mut start = start;
                            let mut end = pattern[p + 2];
                            if start > end {
                                std::mem::swap(&mut start, &mut end);
                            }
                            p += 2;
                            let c = string[0];
                            if c >= start && c <= end {
                                match_found = true;
                            }
                        }
                        Some(&ch) if ch == string[0] => match_found = true,
                        _ => (),
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

#[derive(Debug, thiserror::Error)]
pub enum SplitArgsError {
    #[error("unbalanced quotes")]
    UnbalancedQuotes,
}

pub fn split_args(mut line: &[u8]) -> Result<Vec<Vec<u8>>, SplitArgsError> {
    enum State {
        Normal,
        InDoubleQuotes,
        InSingleQuotes,
    }

    let mut tokens = Vec::new();
    loop {
        match line.find_not_byteset(b" \n\r\t\x0b\x0c") {
            Some(i) => line = &line[i..],
            None => return Ok(tokens), // line is empty, or line consists only of space characters
        }
        let mut state = State::Normal;
        let mut current = Vec::new();
        loop {
            match state {
                State::Normal => match line.first() {
                    Some(b' ' | b'\n' | b'\r' | b'\t') => {
                        line = &line[1..];
                        break;
                    }
                    None => break,
                    Some(b'"') => state = State::InDoubleQuotes,
                    Some(b'\'') => state = State::InSingleQuotes,
                    Some(ch) => current.push(*ch),
                },
                State::InDoubleQuotes => match line {
                    [b'\\', b'x', a, b, _rest @ ..]
                        if a.is_ascii_hexdigit() && b.is_ascii_hexdigit() =>
                    {
                        current.push(hex_digit_to_int(*a) * 16 + hex_digit_to_int(*b));
                        line = &line[3..];
                    }
                    [b'\\', ch, _rest @ ..] => {
                        let byte = match *ch {
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'b' => 0x8,
                            b'a' => 0x7,
                            ch => ch,
                        };
                        current.push(byte);
                        line = &line[1..];
                    }
                    [b'"', next, _rest @ ..] if !is_space(*next) => {
                        return Err(SplitArgsError::UnbalancedQuotes)
                    }
                    [b'"', _rest @ ..] => {
                        line = &line[1..];
                        break;
                    }
                    [] => return Err(SplitArgsError::UnbalancedQuotes),
                    [ch, _rest @ ..] => current.push(*ch),
                },
                State::InSingleQuotes => match line {
                    [b'\\', b'\'', _rest @ ..] => {
                        current.push(b'\'');
                        line = &line[1..];
                    }
                    [b'\'', next, _rest @ ..] if !is_space(*next) => {
                        return Err(SplitArgsError::UnbalancedQuotes)
                    }
                    [b'\'', _rest @ ..] => {
                        line = &line[1..];
                        break;
                    }
                    [] => return Err(SplitArgsError::UnbalancedQuotes),
                    [ch, _rest @ ..] => current.push(*ch),
                },
            }
            line = &line[1..];
        }
        tokens.push(current);
    }
}

// isspace() in C
const fn is_space(ch: u8) -> bool {
    matches!(ch, b' ' | b'\n' | b'\r' | b'\t' | 0xb | 0xc)
}

fn hex_digit_to_int(ch: u8) -> u8 {
    match ch {
        b'0'..=b'9' => ch - b'0',
        b'a'..=b'f' => ch - b'a' + 10,
        b'A'..=b'F' => ch - b'A' + 10,
        _ => unreachable!(),
    }
}

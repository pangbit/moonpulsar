// Native zlib fallback for valid DEFLATE streams the MoonBit flate decoder
// cannot decode.

///|
#borrow(input)
extern "C" fn uncompress_exact(
  input : Bytes,
  input_length : Int,
  output_length : Int,
) -> Bytes = "moonpulsar_zlib_uncompress_exact"

///|
pub fn decompress_exact(input : Bytes, output_length : Int) -> Bytes? {
  if output_length <= 0 {
    return None
  }
  let output = uncompress_exact(input, input.length(), output_length)
  if output.length() == output_length {
    Some(output)
  } else {
    None
  }
}

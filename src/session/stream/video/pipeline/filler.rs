//! The encoder's padding, stripped from each frame before it is packetized.
//!
//! In CBR mode RADV pads every frame up to the bitrate budget (Mesa
//! `radv_video_enc.c` sets `enabled_filler_data`): H.264 filler NAL units
//! (type 12), HEVC filler NAL units (type 38) and AV1 padding OBUs (type 15).
//! A decoder discards them, so sending them only costs airtime.

use memchr::memmem;

use super::hdr_sei::leb128_decode;
use crate::session::stream::video::VideoFormat;

const H264_FILLER_DATA: u8 = 12;
const HEVC_FD_NUT: u8 = 38;
const AV1_OBU_PADDING: u8 = 15;

/// Removes filler NAL units (H.264, HEVC) or padding OBUs (AV1) from an
/// encoded frame in place.
pub(crate) fn strip_filler(data: &mut Vec<u8>, format: VideoFormat) {
	let kept = match format {
		VideoFormat::H264 => compact_nal_units(data, |header| header & 0x1f == H264_FILLER_DATA),
		VideoFormat::Hevc => compact_nal_units(data, |header| (header >> 1) & 0x3f == HEVC_FD_NUT),
		VideoFormat::Av1 => compact_obus(data),
	};
	data.truncate(kept);
}

/// Moves the Annex B NAL units whose first header byte `is_filler` rejects, with
/// their start codes, to the front in order. Returns their length.
fn compact_nal_units(data: &mut [u8], is_filler: impl Fn(u8) -> bool) -> usize {
	let start_code = memmem::Finder::new(&[0, 0, 1]);
	let Some(mut code) = start_code.find(data) else {
		return data.len();
	};
	// A four-byte start code's leading zero belongs to its NAL unit.
	let mut begin = if code > 0 && data[code - 1] == 0 {
		code - 1
	} else {
		code
	};
	// Anything before the first start code stays where it is.
	let mut kept = begin;
	loop {
		let payload = code + 3;
		let next = start_code.find(&data[payload..]).map(|offset| payload + offset);
		let end = match next {
			Some(next) if next > payload && data[next - 1] == 0 => next - 1,
			Some(next) => next,
			None => data.len(),
		};
		if !(payload < data.len() && is_filler(data[payload])) {
			if begin != kept {
				data.copy_within(begin..end, kept);
			}
			kept += end - begin;
		}
		match next {
			Some(next) => {
				code = next;
				begin = end;
			},
			None => return kept,
		}
	}
}

/// Moves the OBUs other than padding to the front in order. Returns their length.
/// An OBU that runs past the end, and whatever follows it, is kept as it is.
fn compact_obus(data: &mut [u8]) -> usize {
	let (mut read, mut kept) = (0, 0);
	while read < data.len() {
		let header = data[read];
		let has_extension = header & 0x04 != 0;
		let has_size_field = header & 0x02 != 0;
		let end = if has_size_field {
			let size_at = (read + 1 + usize::from(has_extension)).min(data.len());
			let (size, size_len) = leb128_decode(&data[size_at..]);
			(size_at + size_len).saturating_add(usize::try_from(size).unwrap_or(usize::MAX))
		} else {
			data.len()
		};
		if end > data.len() {
			break;
		}
		if (header >> 3) & 0x0f != AV1_OBU_PADDING {
			if read != kept {
				data.copy_within(read..end, kept);
			}
			kept += end - read;
		}
		read = end;
	}
	if read != kept {
		data.copy_within(read.., kept);
	}
	kept + (data.len() - read)
}

#[cfg(test)]
mod tests {
	use super::strip_filler;
	use crate::session::stream::video::VideoFormat;

	// Three frames of a black 256x144 picture from RADV at 400 kbps, 120 fps, CBR
	// (an IDR, then two P-frames padded to the budget), and the same with the
	// filler removed. ffmpeg decodes each pair to the same frames (framemd5), and
	// its trace_headers finds no type 38/12/15 unit in the stripped ones.
	const RADV_HEVC: &[u8] = include_bytes!("testdata/radv-hevc-black.h265");
	const RADV_HEVC_STRIPPED: &[u8] = include_bytes!("testdata/radv-hevc-black.stripped.h265");
	const RADV_H264: &[u8] = include_bytes!("testdata/radv-h264-black.h264");
	const RADV_H264_STRIPPED: &[u8] = include_bytes!("testdata/radv-h264-black.stripped.h264");
	const RADV_AV1: &[u8] = include_bytes!("testdata/radv-av1-black.obu");
	const RADV_AV1_STRIPPED: &[u8] = include_bytes!("testdata/radv-av1-black.stripped.obu");

	fn stripped(data: &[u8], format: VideoFormat) -> Vec<u8> {
		let mut data = data.to_vec();
		strip_filler(&mut data, format);
		data
	}

	#[test]
	fn radv_hevc_frames_lose_their_filler() {
		assert_eq!(stripped(RADV_HEVC, VideoFormat::Hevc), RADV_HEVC_STRIPPED);
		assert_eq!((RADV_HEVC.len(), RADV_HEVC_STRIPPED.len()), (879, 182));
	}

	#[test]
	fn radv_h264_frames_lose_their_filler() {
		assert_eq!(stripped(RADV_H264, VideoFormat::H264), RADV_H264_STRIPPED);
		assert_eq!((RADV_H264.len(), RADV_H264_STRIPPED.len()), (828, 109));
	}

	#[test]
	fn radv_av1_frames_lose_their_padding() {
		assert_eq!(stripped(RADV_AV1, VideoFormat::Av1), RADV_AV1_STRIPPED);
		assert_eq!((RADV_AV1.len(), RADV_AV1_STRIPPED.len()), (896, 152));
	}

	#[test]
	fn stripped_frames_stay_as_they_are() {
		assert_eq!(stripped(RADV_HEVC_STRIPPED, VideoFormat::Hevc), RADV_HEVC_STRIPPED);
		assert_eq!(stripped(RADV_H264_STRIPPED, VideoFormat::H264), RADV_H264_STRIPPED);
		assert_eq!(stripped(RADV_AV1_STRIPPED, VideoFormat::Av1), RADV_AV1_STRIPPED);
	}

	#[test]
	fn filler_between_nal_units_goes_and_the_rest_stays_in_order() {
		// HEVC: VPS (32), filler (38) with a 3-byte start code, slice (1), filler.
		let frame = [
			&[0, 0, 0, 1, 0x40, 0x01, 0xaa][..],
			&[0, 0, 1, 0x4c, 0x01, 0xff, 0xff, 0x80],
			&[0, 0, 1, 0x02, 0x01, 0xbb, 0x00, 0x00, 0x03, 0x01],
			&[0, 0, 0, 1, 0x4c, 0x01, 0xff, 0x80],
		]
		.concat();
		let expected = [
			&[0, 0, 0, 1, 0x40, 0x01, 0xaa][..],
			&[0, 0, 1, 0x02, 0x01, 0xbb, 0x00, 0x00, 0x03, 0x01],
		]
		.concat();
		assert_eq!(stripped(&frame, VideoFormat::Hevc), expected);
	}

	#[test]
	fn h264_frame_without_filler_is_untouched() {
		// SPS (7) and a P slice (1); then the same frame with a filler NAL unit (12).
		let frame = [0, 0, 0, 1, 0x67, 0x42, 0, 0, 1, 0x41, 0x9a, 0x02];
		assert_eq!(stripped(&frame, VideoFormat::H264), frame);
		let padded = [&frame[..], &[0, 0, 0, 1, 0x0c, 0xff, 0xff, 0x80]].concat();
		assert_eq!(stripped(&padded, VideoFormat::H264), frame);
	}

	#[test]
	fn av1_padding_without_a_size_field_runs_to_the_end() {
		// Temporal delimiter (size 0), a frame OBU of 2 bytes, then a padding OBU
		// without obu_has_size_field, which takes the rest of the frame.
		let frame = [0x12, 0x00, 0x32, 0x02, 0xaa, 0xbb, 0x78, 0xff, 0xff, 0xff];
		assert_eq!(stripped(&frame, VideoFormat::Av1), [0x12, 0x00, 0x32, 0x02, 0xaa, 0xbb]);
	}
}

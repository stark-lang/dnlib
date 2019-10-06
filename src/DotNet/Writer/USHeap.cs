// dnlib: See LICENSE.txt for more info

﻿using System;
using System.Collections.Generic;
 using System.Diagnostics;
 using System.IO;
using dnlib.IO;
using dnlib.DotNet.MD;

namespace dnlib.DotNet.Writer {
	/// <summary>
	/// #US heap
	/// </summary>
	public sealed class USHeap : HeapBase, IOffsetHeap<string> {
		readonly Dictionary<string, uint> cachedDict = new Dictionary<string, uint>(StringComparer.Ordinal);
		readonly List<string> cached = new List<string>();
		uint nextOffset = 1;
		byte[] originalData;
		Dictionary<uint, byte[]> userRawData;

		/// <inheritdoc/>
		public override string Name => "#US";

		/// <summary>
		/// Populates strings from an existing <see cref="USStream"/> (eg. to preserve
		/// string tokens)
		/// </summary>
		/// <param name="usStream">The #US stream with the original content</param>
		public void Populate(USStream usStream) {
			if (originalData != null)
				throw new InvalidOperationException("Can't call method twice");
			if (nextOffset != 1)
				throw new InvalidOperationException("Add() has already been called");
			if (usStream == null || usStream.StreamLength == 0)
				return;

			var reader = usStream.CreateReader();
			originalData = reader.ToArray();
			nextOffset = (uint)originalData.Length;
			Populate(ref reader);
		}

		void Populate(ref DataReader reader) {
			reader.Position = 1;
			while (reader.Position < reader.Length) {
				uint offset = (uint)reader.Position;
				if (!reader.TryReadCompressedUInt32(out uint len)) {
					if (offset == reader.Position)
						reader.Position++;
					continue;
				}
				if (len == 0 || (ulong)reader.Position + len > reader.Length)
					continue;

				int stringLen = (int)len - 1;
				var s = reader.ReadUtf8String(stringLen);
				var lastByte = reader.ReadByte();
				if (lastByte != 0) {
					throw new InvalidOperationException("Invalid non-zero terminated UTF8 string");
				}

				if (!cachedDict.ContainsKey(s))
					cachedDict[s] = offset;
			}
		}

		/// <summary>
		/// Adds a string to the #US heap
		/// </summary>
		/// <param name="s">The string</param>
		/// <returns>The offset of the string in the #US heap</returns>
		public uint Add(string s) {
			if (isReadOnly)
				throw new ModuleWriterException("Trying to modify #US when it's read-only");
			if (s == null)
				s = string.Empty;

			if (cachedDict.TryGetValue(s, out uint offset))
				return offset;
			return AddToCache(s);
		}

		/// <summary>
		/// Adds a string to the #US heap
		/// </summary>
		/// <param name="s">The string</param>
		/// <returns>The offset of the string in the #US heap</returns>
		public uint Create(string s) {
			if (isReadOnly)
				throw new ModuleWriterException("Trying to modify #US when it's read-only");
			return AddToCache(s ?? string.Empty);
		}

		uint AddToCache(string s) {
			uint offset;
			cached.Add(s);
			cachedDict[s] = offset = nextOffset;
			nextOffset += (uint)GetRawDataSize(s);
			if (offset > 0x00FFFFFF)
				throw new ModuleWriterException("#US heap is too big");
			return offset;
		}

		/// <inheritdoc/>
		public override uint GetRawLength() => nextOffset;

		/// <inheritdoc/>
		protected override void WriteToImpl(DataWriter writer) {
			if (originalData != null)
				writer.WriteBytes(originalData);
			else
				writer.WriteByte(0);

			uint offset = originalData != null ? (uint)originalData.Length : 1;
			foreach (var s in cached) {
				int rawLen = GetRawDataSize(s);
				if (userRawData != null && userRawData.TryGetValue(offset, out var rawData)) {
					if (rawData.Length != rawLen)
						throw new InvalidOperationException("Invalid length of raw data");
					writer.WriteBytes(rawData);
				}
				else
					WriteString(writer, s);
				offset += (uint)rawLen;
			}
		}

		void WriteString(DataWriter writer, string s) {
			writer.WriteCompressedUInt32((uint)GetUTF8ByteCount(s) + 1);
			WriteUTF8(s, writer);
			writer.WriteByte(0);
		}

		/// <inheritdoc/>
		public int GetRawDataSize(string data) {
			var byteCount = GetUTF8ByteCount(data);
			return DataWriter.GetCompressedUInt32Length((uint)byteCount + 1) + byteCount + 1;
		}

		/// <inheritdoc/>
		public void SetRawData(uint offset, byte[] rawData) {
			if (userRawData == null)
				userRawData = new Dictionary<uint, byte[]>();
			userRawData[offset] = rawData ?? throw new ArgumentNullException(nameof(rawData));
		}

		/// <inheritdoc/>
		public IEnumerable<KeyValuePair<uint, byte[]>> GetAllRawData() {
			var memStream = new MemoryStream();
			var writer = new DataWriter(memStream);
			uint offset = originalData != null ? (uint)originalData.Length : 1;
			foreach (var s in cached) {
				memStream.Position = 0;
				memStream.SetLength(0);
				WriteString(writer, s);
				yield return new KeyValuePair<uint, byte[]>(offset, memStream.ToArray());
				offset += (uint)memStream.Length;
			}	
		}

		static void WriteUTF8(string str, DataWriter writer) {
			const char ReplacementCharacter = '\uFFFD';

			for(int i = 0; i < str.Length; i++) {
				char c = str[i];

				if (c < 0x80) {
					writer.WriteByte((byte)c);
					continue;
				}

				if (c < 0x800) {
					writer.WriteByte((byte)(((c >> 6) & 0x1F) | 0xC0));
					writer.WriteByte((byte)((c & 0x3F) | 0x80));
					continue;
				}

				if (IsSurrogateChar(c)) {
					// surrogate pair
					if (IsHighSurrogateChar(c) && i + 1 < str.Length && IsLowSurrogateChar(str[i + 1])) {
						int highSurrogate = c;
						i++;
						int lowSurrogate = str[i];
						int codepoint = (((highSurrogate - 0xd800) << 10) + lowSurrogate - 0xdc00) + 0x10000;
						writer.WriteByte((byte)(((codepoint >> 18) & 0x7) | 0xF0));
						writer.WriteByte((byte)(((codepoint >> 12) & 0x3F) | 0x80));
						writer.WriteByte((byte)(((codepoint >> 6) & 0x3F) | 0x80));
						writer.WriteByte((byte)((codepoint & 0x3F) | 0x80));
						continue;
					}

					// unpaired high/low surrogate
					c = ReplacementCharacter;
				}

				writer.WriteByte((byte)(((c >> 12) & 0xF) | 0xE0));
				writer.WriteByte((byte)(((c >> 6) & 0x3F) | 0x80));
				writer.WriteByte((byte)((c & 0x3F) | 0x80));
			}
		}

		static unsafe int GetUTF8ByteCount(string str) {
			fixed (char* ptr = str) {
				return GetUTF8ByteCount(ptr, str.Length);
			}
		}

		static unsafe int GetUTF8ByteCount(char* str, int charCount) {
			char* remainder;
			return GetUTF8ByteCount(str, charCount, int.MaxValue, out remainder);
		}

		static unsafe int GetUTF8ByteCount(char* str, int charCount, int byteLimit, out char* remainder) {
			char* end = str + charCount;

			char* ptr = str;
			int byteCount = 0;
			while (ptr < end) {
				int characterSize;
				char c = *ptr++;
				if (c < 0x80) {
					characterSize = 1;
				}
				else if (c < 0x800) {
					characterSize = 2;
				}
				else if (IsHighSurrogateChar(c) && ptr < end && IsLowSurrogateChar(*ptr)) {
					// surrogate pair:
					characterSize = 4;
					ptr++;
				}
				else {
					characterSize = 3;
				}

				if (byteCount + characterSize > byteLimit) {
					ptr -= (characterSize < 4) ? 1 : 2;
					break;
				}

				byteCount += characterSize;
			}

			remainder = ptr;
			return byteCount;
		}

		static bool IsSurrogateChar(int c) => unchecked((uint)(c - 0xD800)) <= 0xDFFF - 0xD800;

		static bool IsHighSurrogateChar(int c) => unchecked((uint)(c - 0xD800)) <= 0xDBFF - 0xD800;

		static bool IsLowSurrogateChar(int c) => unchecked((uint)(c - 0xDC00)) <= 0xDFFF - 0xDC00;
	}
}

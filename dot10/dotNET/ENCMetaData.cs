﻿using System;
using dot10.PE;

namespace dot10.dotNET {
	/// <summary>
	/// Used when a #- stream is present in the metadata
	/// </summary>
	class ENCMetaData : MetaData {
		/// <inheritdoc/>
		public ENCMetaData(IPEImage peImage, ImageCor20Header cor20Header, MetaDataHeader mdHeader)
			: base(peImage, cor20Header, mdHeader) {
		}

		/// <inheritdoc/>
		protected override void Initialize() {
			var mdRva = cor20Header.MetaData.VirtualAddress;
			foreach (var sh in mdHeader.StreamHeaders) {
				var rva = mdRva + sh.Offset;
				var imageStream = peImage.CreateStream(rva, sh.Size);
				switch (sh.Name) {
				case "#Strings":
					if (stringsStream == null) {
						allStreams.Add(stringsStream = new StringsStream(imageStream, sh));
						continue;
					}
					break;

				case "#US":
					if (usStream == null) {
						allStreams.Add(usStream = new USStream(imageStream, sh));
						continue;
					}
					break;

				case "#Blob":
					if (blobStream == null) {
						allStreams.Add(blobStream = new BlobStream(imageStream, sh));
						continue;
					}
					break;

				case "#GUID":
					if (guidStream == null) {
						allStreams.Add(guidStream = new GuidStream(imageStream, sh));
						continue;
					}
					break;

				case "#-":
					if (tablesStream == null) {
						allStreams.Add(tablesStream = new ENCTablesStream(imageStream, sh));
						continue;
					}
					break;
				}
				allStreams.Add(new DotNetStream(imageStream, sh));
			}

			if (tablesStream == null)
				throw new BadImageFormatException("Missing MD stream");
			tablesStream.Initialize(peImage);
		}
	}
}
import re
import sys

from lxml import etree

files = sys.argv[1:]

with open("combined.xml", "wb") as f_out:
    root_tag = "ClinVarVariationRelease"
    wrote_opener = False
    # For the first file, read the contents as XML
    for filename in files:
        with open(filename, "rb") as f_in:
            # Use elementtree iterparse to read the file element by element

            for event, elem in etree.iterparse(f_in, events=["start", "end"]):
                if event == "start" and elem.tag == root_tag:
                    print(event, elem)
                    if not wrote_opener:
                        shallow_copy = etree.Element(
                            elem.tag, attrib=elem.attrib, nsmap=elem.nsmap
                        )
                        # Serialize the shallow copy to simulate the original tag without children
                        opening_tag = etree.tostring(
                            shallow_copy,
                            encoding="unicode",
                            with_tail=False,
                            pretty_print=False,
                            xml_declaration=False,
                        )
                        opening_tag = re.sub(r"/>$", r">", opening_tag)

                        print(f"Opening tag: {opening_tag}")
                        f_out.write(opening_tag.encode("utf-8"))
                        f_out.write(b"\n")
                        wrote_opener = True

                elif event == "end" and elem.tag == "VariationArchive":
                    # new_nsmap = {
                    #     prefix: uri
                    #     for prefix, uri in elem.nsmap.items()
                    #     if prefix != "xsi"
                    # }
                    new_element = etree.Element(
                        elem.tag,
                        attrib=elem.attrib,
                        nsmap={},
                    )
                    # Copy text and tail content
                    new_element.text = elem.text
                    new_element.tail = elem.tail

                    # Copy children elements
                    for child in elem:
                        new_element.append(child)

                    elem_s = etree.tostring(new_element)
                    f_out.write(elem_s)
                    f_out.write(b"\n")

                    elem.clear()

    if wrote_opener:
        # Write the closing tag
        f_out.write(b"\n")
        f_out.write(f"</{root_tag}>".encode())

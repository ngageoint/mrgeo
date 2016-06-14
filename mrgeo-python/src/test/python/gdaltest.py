# Taken directly from the gdal app gdalcompare.py, and modified to use
# unittest asserts for the diffs.  Also removed all but the methods, so it
# can't be run from the command line

# ******************************************************************************
#
#  Project:  GDAL
#  Purpose:  Compare two files for differences and report.
#  Author:   Frank Warmerdam, warmerdam@pobox.com
#
# ******************************************************************************
#  Copyright (c) 2012, Frank Warmerdam <warmerdam@pobox.com>
#
#  Permission is hereby granted, free of charge, to any person obtaining a
#  copy of this software and associated documentation files (the "Software"),
#  to deal in the Software without restriction, including without limitation
#  the rights to use, copy, modify, merge, publish, distribute, sublicense,
#  and/or sell copies of the Software, and to permit persons to whom the
#  Software is furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included
#  in all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
#  OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
#  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
#  DEALINGS IN THE SOFTWARE.
# ******************************************************************************

from osgeo import gdal, osr
import math
import numpy


#######################################################
def compare_metadata(golden_md, test_md, id):
    if golden_md is None and test_md is None:
        return 0

    if len(list(golden_md.keys())) != len(list(test_md.keys())):
        print('Difference in %s metadata key count' % id)
        print('  Golden Keys: ' + str(list(golden_md.keys())))
        print('  Test Keys: ' + str(list(test_md.keys())))

    for key in list(golden_md.keys()):
        if key not in test_md:
            print('Test %s metadata lacks key \"%s\"' % (id, key))
        elif test_md[key] != golden_md[key]:
            print('Metadata value difference for key "' + key + '"')
            print('  Golden: "' + golden_md[key] + '"')
            print('  Test:    "' + test_md[key] + '"')


#######################################################
# Review and report on the actual image pixels that differ.
def compare_image_pixels(test, golden_band, test_band):

    diffs = 0
    maxdiff = 0
    nandiff = 0

    for line in range(golden_band.YSize):
        gline = golden_band.ReadAsArray(0, line, golden_band.XSize, 1)[0]
        tline = test_band.ReadAsArray(0, line, test_band.XSize, 1)[0]

        same = numpy.allclose(gline, tline, equal_nan=True)

        if not same:
            for x in range(0, golden_band.XSize):
                g = gline[x]
                t = tline[x]
                if math.isnan(g) != math.isnan(t):
                    diffs += 1
                    nandiff += 1
                elif not math.isnan(g) and g != t:
                    diffs += 1
                    maxdiff = max(maxdiff, gline[x] - tline[x])

    test.assertEqual(diffs, 0, 'Pixels Differing: ' + str(diffs) +
                     ' Maximum Pixel Difference: ' + str(maxdiff) +
                     ' NaN Pixels Differing: ' + str(nandiff))


#######################################################
def compare_band(test, golden_band, new_band, id):
    test.assertEqual(golden_band.DataType, new_band.DataType, ('Band %s pixel types differ.\n' % id) +
                     '  Golden: ' + gdal.GetDataTypeName(golden_band.DataType) + "\n" +
                     '  New:    ' + gdal.GetDataTypeName(new_band.DataType))

    gn = golden_band.GetNoDataValue()
    nn = new_band.GetNoDataValue()
    test.assertEqual(math.isnan(gn), math.isnan(nn), ('Band %s nodata values differ.\n' % id) +
                     '  Golden: ' + str(golden_band.GetNoDataValue()) + '\n' +
                     '  New:    ' + str(new_band.GetNoDataValue()))

    if not math.isnan(gn) and not math.isnan(nn):
        test.assertEqual(golden_band.GetNoDataValue(), new_band.GetNoDataValue(), ('Band %s nodata values differ.\n' % id) +
                         '  Golden: ' + str(golden_band.GetNoDataValue()) + '\n' +
                         '  New:    ' + str(new_band.GetNoDataValue()))

    # Can't check color interpolation, Geotiff save sets it to Gray
    # if golden_band.GetColorInterpretation() != new_band.GetColorInterpretation():
    #     print('Band %s color interpretation values differ.' % id)
    #     print('  Golden: ' + gdal.GetColorInterpretationName(golden_band.GetColorInterpretation()))
    #     print('  New:    ' + gdal.GetColorInterpretationName(new_band.GetColorInterpretation()))

    test.assertEqual( golden_band.Checksum(), new_band.Checksum(), ('Band %s checksum difference:\n' % id) +
                      '  Golden: ' + str(golden_band.Checksum()) + '\n' +
                      '  New:    ' + str(new_band.Checksum()))

    compare_image_pixels(test, golden_band, new_band)

    # Check overviews
    if golden_band.GetOverviewCount() != new_band.GetOverviewCount():
        print('Band %s overview count difference:' % id)
        print('  Golden: ' + str(golden_band.GetOverviewCount()))
        print('  New:    ' + str(new_band.GetOverviewCount()))
    else:
        for i in range(golden_band.GetOverviewCount()):
            compare_band(golden_band.GetOverview(i),
                         new_band.GetOverview(i),
                         id + ' overview ' + str(i))

    # Just like the Dataset, can't compare band metadata...
    # Metadata
    # compare_metadata(golden_band.GetMetadata(), new_band.GetMetadata(), 'Band ' + id)

    # TODO: Color Table, gain/bias, units, blocksize, mask, min/max


#######################################################
def compare_srs(test, golden_wkt, new_wkt):
    if golden_wkt == new_wkt:
        return

    golden_srs = osr.SpatialReference(golden_wkt)
    new_srs = osr.SpatialReference(new_wkt)

    test.assertTrue(golden_srs.IsSame(new_srs), "SRS's differ: \ngolden:\n" +
                    golden_srs.ExportToPrettyWkt() + "\ntest:\n" +
                    new_srs.ExportToPrettyWkt())


#######################################################
def compare_db(test, golden_db, new_db):
    # SRS
    compare_srs(test, str(golden_db.GetProjection()), str(new_db.GetProjection()))

    # GeoTransform
    golden_gt = golden_db.GetGeoTransform()
    new_gt = new_db.GetGeoTransform()

    for i in range(0, len(golden_gt)):
        test.assertAlmostEqual(golden_gt[i], new_gt[i], 7, "GeoTransforms Differ: \ngolden:\n" +
                               str(golden_gt) + "\ntest:\n" +
                               str('(' + ', '.join(str(x) for x in new_db.GetGeoTransform()) + ')'))

    # Can't compare metadata, because the GeoTiff save actually adds keys.  Yuck!
    # Metadata
    # compare_metadata(golden_db.GetMetadata(),new_db.GetMetadata_Dict(''), 'Dataset')

    # Bands
    test.assertEqual(golden_db.RasterCount, new_db.RasterCount, 'Band count mismatch (golden=%d, new=%d)' \
                     % (golden_db.RasterCount, new_db.RasterCount))

    test.assertEqual(golden_db.RasterXSize, new_db.RasterXSize,
                     'Image dimension mismatch (golden=%dx%d, new=%dx%d)' \
                     % (golden_db.RasterXSize, golden_db.RasterYSize,
                        new_db.RasterXSize, new_db.RasterYSize))
    test.assertEqual(golden_db.RasterYSize, new_db.RasterYSize,
                     'Image dimension mismatch (golden=%dx%d, new=%dx%d)' \
                     % (golden_db.RasterXSize, golden_db.RasterYSize,
                        new_db.RasterXSize, new_db.RasterYSize))

    for i in range(golden_db.RasterCount):
        compare_band(test, golden_db.GetRasterBand(i + 1), new_db.GetRasterBand(i + 1), str(i + 1))

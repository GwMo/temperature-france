# "%PROGRAMFILES%\ArcGIS\Pro\bin\Python\Scripts\propy"

# Convert IGN BDALTI ASC files to TIF

import os
import arcpy

root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
ign_dir = os.path.join(root, "0.raw", "ign", "BDALTIV2_2-0_25M_ASC_LAMB93-IGN69")
os.chdir(ign_dir)
for _, departments, _ in os.walk(ign_dir):
	for department in departments:
		dept_dir = os.path.join(ign_dir, department)
		for file in os.listdir(dept_dir):
			if file.endswith('.asc'):
				print(file)
				name, ext = os.path.splitext(file)
				asc = os.path.join(dept_dir, file)
				tif = os.path.join(dept_dir, name + '.tif')

				# define the ascii projection as LAMB93 / IGN69
				print('  projecting')
				arcpy.DefineProjection_management(asc, "PROJCS['RGF_1993_Lambert_93',GEOGCS['GCS_RGF_1993',DATUM['D_RGF_1993',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['False_Easting',700000.0],PARAMETER['False_Northing',6600000.0],PARAMETER['Central_Meridian',3.0],PARAMETER['Standard_Parallel_1',44.0],PARAMETER['Standard_Parallel_2',49.0],PARAMETER['Latitude_Of_Origin',46.5],UNIT['Meter',1.0]],VERTCS['NGF_IGN69',VDATUM['Nivellement_General_de_la_France_IGN69'],PARAMETER['Vertical_Shift',0.0],PARAMETER['Direction',1.0],UNIT['Meter',1.0]]")

				# convert the ascii to GeoTIFF
				print('  converting to GeoTIFF')
				arcpy.CopyRaster_management(asc, tif, nodata_value="-99999", format="TIFF")

				# remove the original ascii and unneeded metadata files that arcgis creates
				print('  cleaning')
				for f in os.listdir(dept_dir):
					if f.startswith(name) and not f.endswith('.tif'):
						# print('    ' + f)
						os.remove(os.path.join(dept_dir, f))

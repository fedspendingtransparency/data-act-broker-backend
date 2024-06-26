# FPDS Elements

The file [fpds_elements.csv](fpds_elements.csv) contains the paths for all FPDS elements obtained through the FPDS feed.

Elements with no GSDM Name have no equivalent in the D1 file. All paths in the Award feed begin with `award/` and all in the IDV feed begin with `IDV/`. If a column is empty, there is no corresponding element in that feed. The elements with neither award nor IDV feeds were at one point being collected but have since been removed from both feeds. They have not yet been removed from the code so should the path reappear they will begin picking up again.
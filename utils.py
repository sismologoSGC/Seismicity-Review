import os
import glob

def inside_the_polygon(p,pol_points):
    """
    Parameters:
    -----------
    p: tuple
        Point of the event. (lon,lat)

    pol_points: list of tuples
        Each tuple indicates one polygon point (lon,lat).

    Returns: 
    --------
    True inside 
    """
    V = pol_points

    cn = 0  
    V = tuple(V[:])+(V[0],)
    for i in range(len(V)-1): 
        if ((V[i][1] <= p[1] and V[i+1][1] > p[1])   
            or (V[i][1] > p[1] and V[i+1][1] <= p[1])): 
            vt = (p[1] - V[i][1]) / float(V[i+1][1] - V[i][1])
            if p[0] < V[i][0] + vt * (V[i+1][0] - V[i][0]): 
                cn += 1  
    condition= cn % 2  
    
    if condition== 1:   
        return True
    else:
        return False

def inside_bna_polygon(p,bna_folder):
    
    """
    Parameters:
    -----------
    p: tuple
        Point of the event. (lon,lat)

    bna_folder: str
        Path of the folder that contains bna files

    Returns: 
    --------
    True if it is inside.
    """
    
    for volcanic_bna in glob.glob(os.path.join(bna_folder,"*")):
        V= []
        polygon_txt= open(f"{volcanic_bna}","r").readlines()

        for line in polygon_txt[1:]:
            _polygon_tuple= eval(line)
            polygon_tuple=(float(_polygon_tuple[0]),float(_polygon_tuple[1]))
            V.append(polygon_tuple)
        if inside_the_polygon(p,V):
            return True

    return False


#files= glob.glob(os.path.join("/home/lmercado/lmercado/Revision_Sismicidad/model_files/","*"))

def test2(p,model):
    #for f in files:
    for f in glob.glob(os.path.join(model,"*")):
        # Get the molecule name
        name = os.path.basename(f)
        split_filname = name.split('.')
        filename=split_filname[0]

        V= []
        polygon_txt= open(f"{f}","r").readlines()
        

        if filename == 'zona1':
            for line in polygon_txt[1:]:
                _polygon_tuple= eval(line)
                polygon_tuple=(float(_polygon_tuple[0]),float(_polygon_tuple[1]))
                V.append(polygon_tuple)
                #print(V)
            if inside_the_polygon(p, V):
                return True, filename
        if filename == 'zona2':
            for line in polygon_txt[1:]:
                _polygon_tuple= eval(line)
                polygon_tuple=(float(_polygon_tuple[0]),float(_polygon_tuple[1]))
                V.append(polygon_tuple)
                #print(V)

            for f_vmm in glob.glob(os.path.join(model,"*")):
                name_vmm = os.path.basename(f_vmm)
                split_filname_vmm = name_vmm.split('.')
                filename_vmm=split_filname_vmm[0]

                V_vmm= []
                polygon_txt_vmm= open(f"{f_vmm}","r").readlines()

                if filename_vmm == 'zona_vmm':
                    for line_vmm in polygon_txt_vmm[1:]:
                        _polygon_tuple_vmm= eval(line_vmm)
                        polygon_tuple_vmm=(float(_polygon_tuple_vmm[0]),float(_polygon_tuple_vmm[1]))
                        V_vmm.append(polygon_tuple_vmm)

                    if inside_the_polygon(p, V):
                        #    return True,filename_vmm
                        #else:
                        #   return True,filename
                        if not inside_the_polygon(p, V_vmm):
                            return True, filename
                
                    if inside_the_polygon(p, V_vmm):
                            return True, filename_vmm
        if filename == 'zona3':
            for line in polygon_txt[1:]:
                _polygon_tuple= eval(line)
                polygon_tuple=(float(_polygon_tuple[0]),float(_polygon_tuple[1]))
                V.append(polygon_tuple)
                #print(V)
            if inside_the_polygon(p, V):
                return True, filename
        if filename == 'zona4':
            for line in polygon_txt[1:]:
                _polygon_tuple= eval(line)
                polygon_tuple=(float(_polygon_tuple[0]),float(_polygon_tuple[1]))
                V.append(polygon_tuple)
                #print(V)
            if inside_the_polygon(p, V):
                return True, filename
        if filename == 'zona5':
            for line in polygon_txt[1:]:
                _polygon_tuple= eval(line)
                polygon_tuple=(float(_polygon_tuple[0]),float(_polygon_tuple[1]))
                V.append(polygon_tuple)
                #print(V)
            if inside_the_polygon(p, V):
                return True, filename
        if filename == 'zona_vmmm':
            for line in polygon_txt[1:]:
                _polygon_tuple= eval(line)
                polygon_tuple=(float(_polygon_tuple[0]),float(_polygon_tuple[1]))
                V.append(polygon_tuple)
                #print(V)
            if inside_the_polygon(p, V):
                return True, filename
        if filename == 'Modelo_CARMA':
            for line in polygon_txt[1:]:
                _polygon_tuple= eval(line)
                polygon_tuple=(float(_polygon_tuple[0]),float(_polygon_tuple[1]))
                V.append(polygon_tuple)
                #print(V)
            if inside_the_polygon(p, V):
                return True, filename
        if filename == 'Modelo_Cesar':
            for line in polygon_txt[1:]:
                _polygon_tuple= eval(line)
                polygon_tuple=(float(_polygon_tuple[0]),float(_polygon_tuple[1]))
                V.append(polygon_tuple)
                #print(V)
            if inside_the_polygon(p, V):
                return True, filename
    return False


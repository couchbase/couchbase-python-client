import libxml2
import os
import re
pycbc_filter=re.compile(r'.*(pycbc|lcb).*')
supp_file = "jenkins/suppressions.txt"
with open(supp_file,"w+") as x:
    for root, dir, files in os.walk("build/valgrind"):
        for file in files:
            fullpath=os.path.join(root,file)
            print("got file: {}".format(file))
            if not file.endswith(".xml"):
                continue

            print("Parsing {}".format(file))
            file=libxml2.parseFile(fullpath)
            suppressions=file.xpathEval2("//suppression")
            print("got suppressions: {}".format(suppressions))
            pycbc=[]
            non_pycbc=[]
            for suppression in suppressions:
                print("evaluating suppression:{}".format(repr(suppression)))
                funs = suppression.xpathEval2("sframe/fun")
                if len(filter(pycbc_filter.match,map(libxml2.xmlNode.content.__get__,funs))):
                    pycbc+=suppression
                else:
                    x.writelines(list(map(libxml2.xmlNode.content.__get__,suppression.xpathEval2("rawtext"))))
                    non_pycbc+=suppression



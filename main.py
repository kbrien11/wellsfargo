import pandas as pd
import os
from flask import Flask,jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route('/', methods=['GET'])
def test():
    # all you have to do is set up local paths for the files add new files to these folders and it will automativally grab them and add them to the consolidated output
    files = []
    for filename in os.listdir("/Users/keith_brien/Documents/Projects/wellsfargo/inout/data_source_2"):
        files.append("/Users/keith_brien/Documents/Projects/wellsfargo/inout/data_source_2/"+ filename)
    for filename in os.listdir("/Users/keith_brien/Documents/Projects/wellsfargo/inout/data_source_1"):
        if filename == "material_reference.csv":
            continue
        files.append("/Users/keith_brien/Documents/Projects/wellsfargo/inout/data_source_1/" +filename)
    data_frames = []
    headers = []
    for file in files:
        df = pd.read_csv(file, '[,|]', engine='python')
        # filter out for low worth products
        # df = df[df['worth'] >1.00]
        df['original_file'] = file
        col = df.columns
        #  removing duplicate headers
        for i in col:
            if i in headers:
                continue
            else:
                headers.append(i)
        data_frames.append(df)
    result = pd.concat(data_frames)
    result.to_csv('/Users/keith_brien/Documents/Projects/wellsfargo/output/consolidated_output.1.csv', index=False)
    return jsonify({"done":"files consolidated"})


if __name__ == "__main__":
    app.run(debug=True , port = 5000)

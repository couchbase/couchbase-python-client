## JSON parser for CMake

Module JSONParser.cmake contains macro `sbeParseJson` to parse JSON string stored in given variable (second argument). 
Macro fills given variable (first argument) with list of all names in JSON string. For each name it creates variable and sets the value from JSON string.
To clear created variables use macro `sbeClearJson`.

> **Module does not validate given JSON string. If given JSON is malformed, the macro gives unpredictable result.**

### Example

JSON to parse:
``` json
{"menu": {
    "header": "SVG Viewer",
    "items": [
        {"id": "Open"},
        {"id": "OpenOld", "label": null },
        {"id": "OpenNew", "label": "Open New"},
        null,
        {"id": "ZoomIn", "label": ["Zoom In", "Zoom At"] },
        {"id": "ZoomOut", 
               "label": 
	         { 
	         "short": ["zo", "zout"], 
	         "long":"Zoom Out"
	         }
    	},
        {"id": "OriginalView", "label": "Original View"},
    	null
     ],
    "elements" : ["one", "two", { "number":"three", "Desc": "Number" }, null ]
}}
```

Let assume that above JSON is stored in variable `jsonTest` in CMake script. It can be parsed with following lines of code.
``` cmake
include(JSONParser.cmake)

# name of variable not string itself is passed to macro sbeParseJson
sbeParseJson(example jsonTest)

# Now you can use parsed variables.
foreach(var ${example})
    message("${var} = ${${var}}")
endforeach()

# when you want to access concrete JSON object, use the names in JSON, the array indexes for JSON object MUST be given after '_'
# e.g if you want to store value of object 'id' of first element of array 'items' in object 'menu'
set(FirstItemId ${example.menu.items_0.id})
# e.g if you want to store 1-th element of array 'short' in object 'label' of 5-th element of array items in object menu 
set(ShortLabel ${example.menu.items_5.label.short_0})

# When you are done, clean parsed variables
sbeClearJson(example)
```

Macro `sbeParseJson` creates following variables and its values (`variable name` = `value`).
```
example.menu.header = SVG Viewer
example.menu.items = 0;1;2;3;4;5;6;7
example.menu.items_0.id = Open
example.menu.items_1.id = OpenOld
example.menu.items_1.label = null
example.menu.items_2.id = OpenNew
example.menu.items_2.label = Open New
example.menu.items_3 = null
example.menu.items_4.id = ZoomIn
example.menu.items_4.label = 0;1
example.menu.items_4.label_0 = Zoom In
example.menu.items_4.label_1 = Zoom At
example.menu.items_5.id = ZoomOut
example.menu.items_5.label.short = 0;1
example.menu.items_5.label.short_0 = zo
example.menu.items_5.label.short_1 = zout
example.menu.items_5.label.long = Zoom Out
example.menu.items_6.id = OriginalView
example.menu.items_6.label = Original View
example.menu.items_7 = null
example.menu.elements = 0;1;2;3
example.menu.elements_0 = one
example.menu.elements_1 = two
example.menu.elements_2.number = three
example.menu.elements_2.Desc = Number
example.menu.elements_3 = null
```


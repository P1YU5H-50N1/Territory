import perspective from "https://cdn.jsdelivr.net/npm/@finos/perspective/dist/cdn/perspective.js";

document.addEventListener("DOMContentLoaded", function() {
    // Get the workspace element
    const workspace = document.getElementById("workspace");
  
    // Create a simple dataset
    const data = [
      { id: 1, name: "Item 1", value: 100 },
      { id: 2, name: "Item 2", value: 200 },
      { id: 3, name: "Item 3", value: 300 }
    ];
  
    // Create a Perspective table
    const table = perspective.worker().table(data);
  
    // Configure the workspace
    const config = {
      sizes: [1],
      detail: {
        main: {
          type: "split-area",
          orientation: "horizontal",
          children: [
            {
              type: "tab-area",
              widgets: ["One"],
              currentIndex: 0
            }
          ]
        }
      },
      widgets: {
        One: {
          table: table,
          plugin: "datagrid",
          title: "My Data"
        }
      }
    };
  
    // Load the configuration into the workspace
    workspace.restore(config);
  });
# Step 1: Base image
FROM node:18-alpine

# Step 2: Set working directory
WORKDIR /app

# Step 3: Install dependencies
# We are creating a minimal package.json just to install react-scripts to run the component easily
COPY package.json ./
RUN npm install

# Step 4: Copy the dashboard files
# We will copy the dashboard files into a standard create-react-app structure inside the container
COPY . ./src/dashboard/

# Step 5: Add a basic index.js and index.html to render the Dashboard component
RUN mkdir -p public && \
    echo '<!DOCTYPE html><html lang="en"><head><title>Missing Data Dashboard</title></head><body><noscript>You need to enable JavaScript to run this app.</noscript><div id="root"></div></body></html>' > public/index.html && \
    echo "import React from 'react'; import ReactDOM from 'react-dom/client'; import Dashboard from './dashboard/Dashboard'; const root = ReactDOM.createRoot(document.getElementById('root')); root.render(<React.StrictMode><Dashboard /></React.StrictMode>);" > src/index.js

# Step 6: Expose the port React runs on
EXPOSE 3000

# Step 7: Run the app
CMD ["npm", "start"]
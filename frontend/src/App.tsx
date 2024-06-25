// src/App.tsx
import React from "react";
import "./App.css";
import OrderStatusChart from "./components/OrderStatusChart";

const App: React.FC = () => {
  return (
    <div className="App">
      <header className="App-header">
        <h1>E-commerce Order Status Dashboard</h1>
      </header>
      <main>
        <OrderStatusChart />
      </main>
    </div>
  );
};

export default App;

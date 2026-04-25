import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Modern ETL | AI Data Assistant",
  description: "Enterprise Data Platform and AI Assistant",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}

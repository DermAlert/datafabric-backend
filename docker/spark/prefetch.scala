// Force Spark/Ivy to resolve every runtime package while the image is built.
// Compose copies the resulting cache to the backend before it starts.
System.exit(0)

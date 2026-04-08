import os

filepath = 'app/routes/v1/discover.py'
if os.path.exists(filepath):
    with open(filepath, 'r') as f:
        lines = f.readlines()
        
    new_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        # Remove duplicate service init
        if 'service = ContentService(db)' in line:
            new_lines.append(line)
            if i + 1 < len(lines) and 'service = ContentService(db)' in lines[i+1]:
                i += 1
        # Remove duplicate return
        elif 'return ok({"items": items})' in line:
            new_lines.append(line)
            if i+1 < len(lines) and 'return ok({"items": items})' in lines[i+1]:
                i += 1
        # Change Dict to dict
        else:
            new_lines.append(line.replace('Dict[str, Any]', 'dict[str, Any]'))
        i += 1
        
    with open(filepath, 'w') as f:
        f.writelines(new_lines)
    print("Cleaned up discover.py")
else:
    print("File not found")

with open('streamlit_app.py', 'r') as f:
    lines = f.readlines()

with open('streamlit_app.py', 'w') as f:
    main_block_found = False
    for i, line in enumerate(lines):
        if 'if __name__ == "__main__":' in line:
            if not main_block_found:
                f.write(line)
                main_block_found = True
            # else, do nothing, just skip the line
        else:
            f.write(line)

# Add main() call if the main block was found
if main_block_found:
    # check if the last line already has main()
    with open('streamlit_app.py', 'r') as f:
        all_lines = f.readlines()
        if len(all_lines) > 0:
            last_line = all_lines[-1]
            if 'main()' not in last_line:
                # and make sure that the if __name__ is the last line
                if 'if __name__ == "__main__":' in last_line:
                    with open('streamlit_app.py', 'a') as f_append:
                        f_append.write('    main()\n')
        else:
            with open('streamlit_app.py', 'a') as f_append:
                f_append.write('if __name__ == "__main__":\n    main()\n')

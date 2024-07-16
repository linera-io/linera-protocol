export function convertMillisToDateTime(microseconds: number) {
  const milliseconds = microseconds / 1000

  const date = new Date(milliseconds)

  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')

  let hours = date.getHours()
  const minutes = String(date.getMinutes()).padStart(2, '0')

  const ampm = hours >= 12 ? 'PM' : 'AM'
  hours = hours % 12
  hours = hours ? hours : 12 // the hour '0' should be '12'
  const formattedHours = String(hours).padStart(2, '0')

  const formattedDate = `${year}-${month}-${day}`
  const formattedTime = `${formattedHours}:${minutes} ${ampm}`

  return { formattedDate, formattedTime }
}
